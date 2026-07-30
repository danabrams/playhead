// AdPodContinuation.swift
// playhead-xsdz.65: one banner, whole break — recover the ad-pod NEIGHBOURS of
// an ad we already found.
//
// THE DEFECT THIS ADDRESSES
// -------------------------
// A DAI ad pod is a CHAIN of stitched creatives, not one ad. Measured on
// 2026-07-30 against rediff-confirmed slots (byte-derived pod boundaries, so
// independent of any human labelling): of the 17 slots the pipeline detected at
// all, 14 were covered by exactly ONE pipeline window, mean slot coverage 0.33,
// and 13 slots carried an uncovered run longer than 30 s. 1324 s of ad audio
// sits uncovered INSIDE pods we had already located (994 s of it in those 13
// slots' single largest holes), with a largest single hole of 175 s. A 175 s
// hole is not an edge that stopped short; it is two or three whole extra ads. Concretely, on `conan-2026-07-09` the pipeline emitted
// `[797.2, 810.7)` inside the pod `[~657.6, 810.7)` — it caught the tail of the
// FOURTH ad and left the SiriusXM promo, Carvana and Carter's reads untouched.
// That is exactly what the product owner hears: a banner fires, one ad is
// handled, and the next ad plays.
//
// Distinct from playhead-4xqf (single-span UNDERSIZING, a <30 s edge that
// stopped short). This type only ever adds material that has its OWN ad
// evidence; it never nudges an edge to "look better".
//
// WHAT THIS DOES — AND WHY IT CANNOT EAT THE SHOW
// -----------------------------------------------
// "Keep detecting until content resumes" is a WIDENING rule, and widening into
// show content is the failure this product cannot afford: the inner edges
// matter more than the outer ones, and eating show is far worse than missing an
// ad. So the rule is built so that every second it claims is claimed on
// POSITIVE evidence, and it is stopped by POSITIVE evidence that content
// resumed. Two structural properties do that work:
//
//  1. THE WALK TERMINATES AT EVIDENCE, NEVER PAST IT. From a seed window we
//     step outward link by link, where a LINK is a region carrying commercial
//     signal that is near-exclusive to advertising — a sponsor disclosure, a
//     promo code, offer-terms boilerplate, or a three-role persuasion arc
//     (`RhetoricalGrammarEvidenceBuilder`) — with playhead-xsdz.1's own
//     negative-evidence guardrails applied. See `adCopyLinks` and
//     `rhetoricalLinks`. The claimed extension ends exactly at the last link's
//     end. Nothing beyond the final piece of positive ad evidence is ever
//     claimed, so the rule never reasons "no ad cue was found here, therefore
//     this is still ad" — an absence is never an input.
//
//     BE PRECISE ABOUT THE BRIDGE, because it is the one place the rule is
//     weaker than the sentence above. The seconds BETWEEN two links carry no
//     evidence of their own; they are claimed because they are BRACKETED by
//     positive ad evidence on both sides and no barrier stands between. That is
//     BOUNDED EXTRAPOLATION, not positive evidence per second, and calling it
//     anything else would be the "absence dressed as a quantity" mistake. The
//     only control over it is `maxLinkGapSeconds` — which is why that default is
//     30 s, set by a held-out measurement that caught the bridge crossing a
//     sponsored show segment at 60 s (see `Configuration.maxLinkGapSeconds`), and
//     why the corpus eval asserts a FROZEN budget on seconds claimed outside a
//     byte-confirmed ad slot rather than merely printing it.
//
//  2. A POSITIVE CONTENT-RESUMED BARRIER STOPS THE WALK. A barrier is an
//     AFFIRMATIVE verdict that the audio is show content, not the absence of an
//     ad verdict:
//       • an FM `noAds` scan window over good-quality transcript — the primary
//         classifier was asked about that region and answered "there are no ads
//         here". That is the same signal `FMSuppressionGuard` already trusts to
//         downweight real evidence, used here in the same direction.
//       • an explicit spoken return marker (`LexicalPatternCategory
//         .transitionMarker`: "and now back to the show", "without further
//         ado", …) — the host saying, in words, that the break is over. This is
//         the same cue `TimeBoundaryResolver` already scores as
//         `explicitReturnMarker`.
//     A barrier intersecting the corridor TERMINATES the walk on that side; it
//     does not merely skip one step, because stepping past a wall is crossing
//     it.
//
// SAFETY CONTRACT (each item is pinned by a test)
// -----------------------------------------------
//   • MARK-ONLY, ALWAYS. Every emitted row is a NEW window with a hard-coded
//     `eligibilityGate == .markOnly`, `decisionState == .candidate`, and BOTH
//     edge anchors `.unanchored`. Under playhead-2350 an unanchored edge can
//     never auto-skip, so recovered pod material becomes a BANNER — which is
//     exactly the certainty-tiered policy: auto-skip only the deterministic
//     (byte-exact rediff) material, MARK the rest.
//   • IT NEVER TOUCHES AN EXISTING WINDOW. No seed's geometry, gate, anchors,
//     confidence or id is modified — the pass is purely additive. That makes
//     playhead-ye0n's rule (never demote a whole window for extending one edge)
//     satisfied by construction rather than by argument: there is no edge
//     rewrite and no gate arithmetic anywhere in this file.
//   • IT NEVER CROSSES A LISTENER'S MARK (playhead-lc4c). `protectedRegions`
//     are supplied by the caller because persisted `userMarked` rows are not in
//     the window list this pass receives. A protected region intersecting the
//     corridor terminates the walk, and user-marked rows are never used as
//     seeds — a manual mark outranks anything derived, in both directions.
//
// PURITY: pure functions over value types, `Foundation`-only, deterministic, no
// I/O and no actor hops — the `SpecialistMarkComposer` shape, so the whole
// contract is unit-testable with synthetic rows.

import CryptoKit
import Foundation

enum AdPodContinuation {

    // MARK: - Provenance constants

    /// Detector version stamped on every continuation mark. Reconcile isolation
    /// scopes to this exact string (`AdDetectionService
    /// .isReconcilableBackfillWindow`), so continuation marks and FM
    /// (`detection-v1`) / specialist (`specialist-ft-v2`) marks can never retire
    /// one another.
    static let detectorVersion = "pod-continuation-v1"

    /// `boundaryState` stamped on every continuation mark. A NON-user literal
    /// that MUST stay OUT of `AdDetectionService.reconcileProtectedBoundaryStates`
    /// so this pass can retire its own stale rows on a re-run.
    static let boundaryState = "podContinuation"

    /// `metadataSource` stamped on every continuation mark. Paired with
    /// `metadataConfidence == nil` so `AdBannerView.bannerCopy` uses the generic
    /// "sounds like a sponsor break" copy — we know a pod continues, we do NOT
    /// know which advertiser, and guessing one would be a hallucination.
    ///
    /// CAUSAL ATTRIBUTION IS DEFERRED, and the default is the safe one. This tag
    /// is not in `SkipOrchestrator.causalSource(forMetadataSource:)`, so a veto on
    /// a continuation mark attributes to `.foundationModel`. That is
    /// MISATTRIBUTED but harmless, and deliberately left alone: `.foundationModel`
    /// carries no entry in `CausalSourceDemotionStore`'s rules, so the veto
    /// demotes nothing. Mapping it to the honest `.lexical` instead would be
    /// WORSE — `.lexical` IS demotable (delta 0.20, floor 0.30), so a
    /// false-positive continuation mark would drag down the real lexical
    /// channel's trust on that show, including for spans that DO auto-skip.
    /// Correct attribution needs its own `CausalSource` case, exempt from the
    /// demotion rules for the same reason `.specialist` is (a signal that never
    /// auto-skips has nothing to demote); that is an enum-widening change with
    /// its own exhaustive-switch fallout, not this bead's work.
    static let metadataSource = "pod-continuation-v1"

    // MARK: - Configuration

    struct Configuration: Sendable, Equatable {
        /// Maximum gap (seconds) between the current edge and the next ad-copy
        /// link for the chain to continue. This is the length of UNVERIFIED audio
        /// one step may annex — the bridge between two positive-evidence points —
        /// so it is the single most safety-relevant number in this file.
        ///
        /// 30 s, and it is set by a HELD-OUT measurement overruling an in-sample
        /// one. The two disagreed, and the held-out one won:
        ///
        ///   • IN-SAMPLE (the 2026-07-16 Catalyst corpus, scored against
        ///     rediff-confirmed pod boundaries) wanted 60. Raising the gap
        ///     30 → 45 → 60 recovered 191 → 302 → 617 s of ad audio inside
        ///     detected pods and took the count of pods still carrying a >30 s
        ///     hole from 12 to 7, with ZERO seconds landing outside a rediff slot
        ///     at any setting. On that evidence alone 60 looks free.
        ///
        ///   • HELD OUT (the device database, 21 real episodes, nothing here used
        ///     to pick a parameter) says it is not. At gap 60 the pass claimed
        ///     THEMOVE 2668.6–2708.9, and the transcript for those 40 s is
        ///     "Yesterday's question. When was the white jersey introduced … the
        ///     answer, 1975 … and today's question is" — the show's trivia
        ///     segment, bracketed at both ends by the SAME sponsor's CTA. A
        ///     sponsored segment is not a pod, and 40 s of show became ad. The
        ///     mark appears at gap 45 and is absent at 35.
        ///
        /// So the bridge is capped below the length of a single creative (15–30 s),
        /// which is the principled reading of "the next creative starts about now"
        /// as well as the safe one. 30 rather than 35 deliberately: 35 would be
        /// fitted to that one observation by one second.
        ///
        /// The recall left on the table is real and is not hidden — the corpus eval
        /// keeps every arm, so the 60 s setting and what it would buy stay visible
        /// for the flag-flip decision. Recovering it needs a positive content
        /// signal that can tell a sponsored SEGMENT from a pod, not a wider bridge.
        ///
        /// `<= 0` disables the pass (no chain can ever start).
        var maxLinkGapSeconds: Double

        /// Hard bound on how far one side of one seed may be extended.
        ///
        /// This is a BLAST-RADIUS BOUND, not the safety control — the safety
        /// control is that every step must land on positive ad evidence and
        /// must not cross a content barrier. It exists so that a pathological
        /// hit stream (e.g. a shopping-format show that says "sponsor" and a URL
        /// every 20 s for an hour) cannot produce an hour-long mark. The widest
        /// rediff-confirmed pod in the 2026-07-16 corpus is ~212 s end to end,
        /// so 180 s per side cannot be reached by a legitimate pod chain unless
        /// the seed itself is already most of the pod.
        var maxExtensionSecondsPerSide: Double

        /// Shortest continuation mark worth emitting, in seconds.
        ///
        /// The recovered span is reduced to the material NOTHING already covers
        /// (see ``compose(existingWindows:adCopyLinks:contentBarriers:protectedRegions:episodeDuration:analysisAssetId:config:)``),
        /// and that subtraction can leave sub-second slivers at the edges of an
        /// already-detected creative. A sliver is not an ad the listener hears; it
        /// is arithmetic. 3 s is below any real creative and above the
        /// transcript-interpolation slop that produces the slivers.
        var minMarkDurationSeconds: Double

        /// Ceiling for the confidence stamped on a continuation mark.
        ///
        /// A mark inherits the seed's PRESENCE confidence (the claim is "this is
        /// the same pod"), capped here: a continuation is never more certain
        /// than the ad it continues, and is never presented as more than a
        /// suggestion. 0.70 is `SkipOrchestrator.preloadConfidenceThreshold`, so
        /// a mark from a normally-confident seed still survives cross-launch
        /// preload like every other mark-only row. It is NOT a calibrated
        /// probability and it feeds no gate: the gate is the hard-coded
        /// `.markOnly` literal below.
        var markConfidenceCeiling: Double

        static let `default` = Configuration(
            maxLinkGapSeconds: 30.0,
            maxExtensionSecondsPerSide: 180.0,
            minMarkDurationSeconds: 3.0,
            markConfidenceCeiling: 0.70
        )

        init(
            maxLinkGapSeconds: Double = 30.0,
            maxExtensionSecondsPerSide: Double = 180.0,
            minMarkDurationSeconds: Double = 3.0,
            markConfidenceCeiling: Double = 0.70
        ) {
            self.maxLinkGapSeconds = maxLinkGapSeconds
            self.maxExtensionSecondsPerSide = maxExtensionSecondsPerSide
            self.minMarkDurationSeconds = minMarkDurationSeconds
            self.markConfidenceCeiling = markConfidenceCeiling
        }
    }

    // MARK: - Inputs

    /// A region in which the vetted lexical auto-ad rule FIRED — i.e. positive,
    /// guardrailed evidence that commercial copy is being read here. Produced by
    /// ``adCopyLinks(hits:analysisAssetId:builder:)`` so the authority on "is
    /// this ad copy" stays `LexicalAutoAdEvidenceBuilder` and cannot drift.
    struct AdCopyLink: Sendable, Equatable, Hashable {
        let start: Double
        let end: Double

        init(start: Double, end: Double) {
            self.start = start
            self.end = end
        }
    }

    /// A region POSITIVELY classified as show content. Never "we found no ad
    /// evidence here" — see ``contentBarriers(semanticScanResults:lexicalHits:)``
    /// for the two admissible derivations and why each is an affirmative verdict.
    struct ContentBarrier: Sendable, Equatable, Hashable {
        let start: Double
        let end: Double

        init(start: Double, end: Double) {
            self.start = start
            self.end = end
        }
    }

    /// Decision states that make an existing window "visible" — i.e. real enough
    /// to seed a chain from and to dedupe against. A suppressed or reverted row
    /// is not something the listener sees, so it neither seeds nor blocks.
    /// Mirrors `SpecialistMarkComposer.visibleDecisionStates`.
    static let visibleDecisionStates: Set<String> = [
        AdDecisionState.candidate.rawValue,
        AdDecisionState.confirmed.rawValue,
        AdDecisionState.applied.rawValue
    ]

    /// Decision states that may SEED a continuation chain. Deliberately narrower
    /// than `visibleDecisionStates`: a `.candidate` row is a coarse proposal (the
    /// segment aggregator tiles fixed 30 s regions as candidates), and chaining
    /// off an unconfirmed tile would build a pod claim on a guess. Only a
    /// verdict the pipeline actually confirmed anchors a chain.
    static let seedDecisionStates: Set<String> = [
        AdDecisionState.confirmed.rawValue,
        AdDecisionState.applied.rawValue
    ]

    // MARK: - Link derivation (the positive ad-evidence half)

    /// A KIND of ad-copy evidence. Deliberately coarse: the rule below counts
    /// DISTINCT kinds, and "two different sorts of commercial signal in the same
    /// breath" is a far better precision bar than "many hits of one sort".
    enum AdCopyKind: String, Sendable, Hashable, CaseIterable {
        /// A sponsor disclosure — "brought to you by", "supported by".
        case sponsor
        /// "use code SAVE20", "promo code SHOW".
        case promoCode
        /// A spoken URL or call to action.
        case urlCTA
        /// Offer language — "free trial", "money back guarantee", "20 percent off".
        case purchaseLanguage
        /// Offer-terms BOILERPLATE — "terms and conditions", "while supplies
        /// last", "see website for details". See ``adTermsPatterns``.
        case adTerms

        /// A kind that, on its own, is almost never uttered outside an ad read.
        /// A link needs at least one of these; `urlCTA` and `purchaseLanguage`
        /// are corroborators only, because editorial prose does say "check out
        /// their site" and "there is a free trial".
        var isStrong: Bool {
            switch self {
            case .sponsor, .promoCode, .adTerms: return true
            case .urlCTA, .purchaseLanguage: return false
            }
        }
    }

    /// One located piece of ad-copy evidence.
    struct AdCopySignal: Sendable, Equatable, Hashable {
        let kind: AdCopyKind
        let start: Double
        let end: Double
    }

    /// Offer-terms boilerplate. This family exists because THE DOMINANT MISS IS
    /// NOT A HOST READ.
    ///
    /// The vetted `LexicalAutoAdEvidenceBuilder` rule requires a SPONSOR
    /// DISCLOSURE leg, and a sponsor disclosure is a HOST-READ convention: the
    /// host says "this episode is brought to you by X". A programmatically
    /// stitched DAI creative never says that — it is a finished radio spot. Run
    /// over the 16 uncovered runs longer than 30 s in the 2026-07-16 corpus, only
    /// 3 contained a sponsor disclosure at all, so a sponsor-gated rule fires
    /// almost nowhere in exactly the population this bead is about. (Measured:
    /// the first build of this pass recovered 0.0 s for precisely that reason.)
    ///
    /// What a finished spot DOES carry is offer boilerplate, read fast at the
    /// end: "while supplies last, ends June 30th, see website for more details",
    /// "commutations and exclusions may apply, see our seven day return policy".
    /// These phrases are near-exclusive to advertising — editorial narration does
    /// not disclaim its own terms — which makes them a STRONG kind and the most
    /// show-agnostic DAI text marker available without a per-show bank.
    ///
    /// Deliberately owned by THIS file rather than added to `LexicalScanner`:
    /// the shared lexicon feeds the fusion scorer, and widening it would change
    /// scores everywhere. This pass gets the signal it needs; nothing else moves.
    static let adTermsPatterns: [String] = [
        #"\bterms and conditions\b"#,
        #"\bterms apply\b"#,
        #"\bexclusions (may )?apply\b"#,
        #"\brestrictions (may )?apply\b"#,
        #"\bwhile supplies last\b"#,
        #"\boffer (ends|expires|valid)\b"#,
        #"\bcannot be combined\b"#,
        #"\bat participating\b"#,
        #"\bparticipating (locations|retailers|stores)\b"#,
        #"\bsee (the |our )?(web ?site|site|store)\b[^.]{0,24}\b(details|more)\b"#,
        #"\bsee our\b[^.]{0,28}\b(policy|details)\b"#,
        #"\bfor a limited time\b"#
    ]

    private static let compiledAdTermsPatterns: [NSRegularExpression] =
        adTermsPatterns.compactMap {
            try? NSRegularExpression(pattern: $0, options: [.caseInsensitive])
        }

    /// Locate every piece of ad-copy evidence in the episode.
    ///
    /// Four kinds come from the `LexicalScanner` hit stream the pipeline already
    /// computed (no rescan, no new lexicon). The fifth — `adTerms` — is scanned
    /// here over the transcript, because offer boilerplate is not in the shared
    /// lexicon and must not be added to it.
    ///
    /// `transitionMarker` hits are excluded: a return marker is a CONTENT signal,
    /// and it is consumed as a barrier below. Metadata-origin and negative-pattern
    /// hits are excluded as promotion legs, mirroring the vetted rule.
    ///
    /// An `adTerms` signal is CHUNK-GRANULAR (the whole segment's time span)
    /// rather than character-interpolated. Whisper segments run a few seconds, and
    /// this signal is used to decide chain ADJACENCY, not an edge — the mark's
    /// edges come from the link intervals, which are bounded by the pair. Being
    /// chunk-granular makes the signal slightly WIDER, which can only make a link
    /// slightly wider, so it is not the silent-precision-loss direction.
    static func adCopySignals(
        chunks: [TranscriptChunk],
        hits: [LexicalHit]
    ) -> [AdCopySignal] {
        var signals: [AdCopySignal] = []
        for hit in hits {
            guard !hit.isMetadataOrigin,
                  !hit.isNegativePattern,
                  hit.startTime.isFinite,
                  hit.endTime.isFinite,
                  hit.endTime >= hit.startTime else {
                continue
            }
            let kind: AdCopyKind
            switch hit.category {
            case .sponsor: kind = .sponsor
            case .promoCode: kind = .promoCode
            case .urlCTA: kind = .urlCTA
            case .purchaseLanguage: kind = .purchaseLanguage
            case .transitionMarker: continue
            }
            signals.append(AdCopySignal(kind: kind, start: hit.startTime, end: hit.endTime))
        }
        for chunk in chunks {
            guard chunk.startTime.isFinite,
                  chunk.endTime.isFinite,
                  chunk.endTime > chunk.startTime,
                  !chunk.normalizedText.isEmpty else {
                continue
            }
            let text = chunk.normalizedText
            let range = NSRange(location: 0, length: (text as NSString).length)
            let matched = compiledAdTermsPatterns.contains {
                $0.firstMatch(in: text, range: range) != nil
            }
            if matched {
                signals.append(
                    AdCopySignal(kind: .adTerms, start: chunk.startTime, end: chunk.endTime)
                )
            }
        }
        return signals.sorted {
            $0.start != $1.start ? $0.start < $1.start : $0.end < $1.end
        }
    }

    /// Project ad-copy signals into LINKS: regions where a co-occurrence of
    /// commercial signals says, positively, that a spot is being read.
    ///
    /// THE BAR: at least one STRONG kind — a sponsor disclosure, a promo code,
    /// or offer-terms boilerplate. A second, distinct kind within
    /// `cooccurrenceWindow` WIDENS the link's interval but is not required.
    ///
    /// The weak kinds cannot carry a link alone, which is the precision control
    /// that matters: a brand mentioned editorially is `urlCTA`-only, a self-promo
    /// ("follow the show") is `urlCTA`-only, a product discussion is
    /// `purchaseLanguage` at most. None of those fires anything.
    ///
    /// This is deliberately WEAKER than playhead-xsdz.1's "sponsor AND promo/URL"
    /// bar, and the difference is justified by what each rule is for. xsdz.1
    /// makes a STANDALONE, auto-skip-grade presence claim anywhere in an episode,
    /// so it must be near-infallible. A link here only extends a chain that a
    /// CONFIRMED ad window already anchored, within `maxLinkGapSeconds`, never
    /// across a content barrier, and its output is mark-only. It is answering
    /// "does the break continue?", not "is there an ad here?".
    ///
    /// Measured, on rediff-confirmed pod boundaries: admitting single-strong-kind
    /// links took recovered ad audio inside detected pods from 408 s to 617 s and
    /// the count of pods still carrying a >30 s hole from 11 to 7, while the
    /// newly-claimed seconds landing OUTSIDE every rediff slot stayed at exactly
    /// 23.0 s — i.e. every one of those extra 209 s landed inside a byte-confirmed
    /// DAI insertion. Set `allowSingleStrongKindLinks: false` for the stricter
    /// two-kind bar; the corpus eval keeps that arm as the conservative
    /// comparison.
    ///
    /// Both of the vetted rule's guardrails still apply, and the second is
    /// literally the same function rather than a copy of it:
    ///   • a show-owned-domain negative-pattern hit nearby suppresses (the show
    ///     plugging its OWN site is exactly what must not be treated as an ad);
    ///   • `LexicalAutoAdEvidenceBuilder.hasNegativeContext` suppresses when a
    ///     news / review / editorial cue sits near the pair.
    static func adCopyLinks(
        chunks: [TranscriptChunk],
        hits: [LexicalHit],
        allowSingleStrongKindLinks: Bool = true,
        builder: LexicalAutoAdEvidenceBuilder = LexicalAutoAdEvidenceBuilder(),
        config: LexicalAutoAdEvidenceBuilder.Config = .default
    ) -> [AdCopyLink] {
        let signals = adCopySignals(chunks: chunks, hits: hits)
        guard signals.contains(where: { $0.kind.isStrong }) else { return [] }
        let negativeHits = hits.filter {
            $0.isNegativePattern && $0.startTime.isFinite && $0.endTime.isFinite
        }

        // Candidate link intervals, before the guardrails.
        var candidates: [AdCopyLink] = []
        if allowSingleStrongKindLinks {
            for signal in signals where signal.kind.isStrong {
                candidates.append(AdCopyLink(start: signal.start, end: signal.end))
            }
        }
        for (index, first) in signals.enumerated() {
            for second in signals[(index + 1)...] {
                guard first.kind != second.kind else { continue }
                guard first.kind.isStrong || second.kind.isStrong else { continue }
                guard second.start - first.start <= config.cooccurrenceWindow else { break }
                let start = min(first.start, second.start)
                let end = max(first.end, second.end)
                guard end > start else { continue }
                candidates.append(AdCopyLink(start: start, end: end))
            }
        }
        guard !candidates.isEmpty else { return [] }

        var links: [AdCopyLink] = []
        for candidate in candidates.sorted(by: {
            $0.start != $1.start ? $0.start < $1.start : $0.end < $1.end
        }) {
            // Already inside an accepted link ⇒ nothing to add, and no need to
            // pay for the guardrail evaluation.
            if links.contains(where: { $0.start <= candidate.start && $0.end >= candidate.end }) {
                continue
            }
            let crossesShowOwnedDomain = negativeHits.contains {
                $0.startTime <= candidate.end + config.negativeContextRadius
                    && $0.endTime >= candidate.start - config.negativeContextRadius
            }
            guard !crossesShowOwnedDomain else { continue }
            let center = (candidate.start + candidate.end) / 2.0
            guard !builder.hasNegativeContext(near: center, hits: hits) else { continue }
            links.append(candidate)
        }
        return mergeLinks(links)
    }

    /// Length (seconds) of the prose window handed to the rhetorical-grammar
    /// assessor below. A finished radio spot runs roughly 15–30 s and its
    /// persuasion arc occupies the whole of it, so a 30 s window is the smallest
    /// that can hold a full arc. Windows slide one transcript segment at a time,
    /// so a spot straddling a segment boundary is still seen whole.
    static let rhetoricalProbeWindowSeconds = 30.0

    /// Ad-copy links from the RHETORICAL GRAMMAR — the answer to "what says this
    /// is ad copy when there is no sponsor disclosure and no promo code?"
    ///
    /// `RhetoricalGrammarEvidenceBuilder` (playhead-xsdz.12) exists for exactly
    /// this case: it fires on the ORDERED CO-OCCURRENCE of three or more
    /// persuasion roles (hook → problem → solution → evidence → offer → CTA) and
    /// was written because that "fires even when the existing sponsor /
    /// promo-code / URL lexical cues do NOT". Its own file documents why three
    /// roles is the precision bar: an editorial brand mention is SOLUTION-only, a
    /// self-promo is CTA-only, a product review is EVIDENCE+SOLUTION — none of
    /// them clears it.
    ///
    /// This matters because the corpus says so. The Conan pod's Carter's spot is
    /// "Carter's has your family covered for every summer first … Generations of
    /// families have trusted our must-haves … Visit Carter's dot com to shop the
    /// latest styles" — an entire creative whose only lexical hit is a bare URL,
    /// which is deliberately NOT enough for a link on its own. The prose is
    /// nonetheless unmistakable ad copy, and the grammar is what reads it.
    ///
    /// NOTE this is a DIFFERENT use of the channel from the fusion one, which is
    /// gated OFF pending a corpus A/B (`rhetoricalGrammarEnabled`). There it adds
    /// score mass to a presence verdict. Here it can only extend a chain that a
    /// CONFIRMED ad window already anchored, its output is mark-only, and a
    /// content barrier still stops the walk — so it is doing bounded extent work
    /// next to an established ad rather than deciding presence on its own.
    static func rhetoricalLinks(
        chunks: [TranscriptChunk],
        builder: RhetoricalGrammarEvidenceBuilder = RhetoricalGrammarEvidenceBuilder()
    ) -> [AdCopyLink] {
        let usable = chunks
            .filter {
                $0.startTime.isFinite && $0.endTime.isFinite
                    && $0.endTime > $0.startTime && !$0.text.isEmpty
            }
            .sorted { $0.startTime < $1.startTime }
        guard !usable.isEmpty else { return [] }

        var links: [AdCopyLink] = []
        for startIndex in usable.indices {
            let windowStart = usable[startIndex].startTime
            var endIndex = startIndex
            while endIndex + 1 < usable.count,
                  usable[endIndex + 1].endTime - windowStart <= rhetoricalProbeWindowSeconds {
                endIndex += 1
            }
            // A single segment is prose too short to carry a three-role arc; a
            // one-segment "window" would only ever fire on a freak sentence.
            guard endIndex > startIndex else { continue }
            let windowEnd = usable[endIndex].endTime
            // Already inside an accepted link ⇒ nothing to add.
            if links.contains(where: { $0.start <= windowStart && $0.end >= windowEnd }) {
                continue
            }
            let text = usable[startIndex...endIndex]
                .map(\.text)
                .joined(separator: " ")
            guard builder.assess(text: text) != nil else { continue }
            links.append(AdCopyLink(start: windowStart, end: windowEnd))
        }
        return mergeLinks(links)
    }

    /// Sort and union overlapping/touching links so the chain walk sees one
    /// interval per contiguous run of ad copy.
    static func mergeLinks(_ links: [AdCopyLink]) -> [AdCopyLink] {
        unionIntervals(links.map { (start: $0.start, end: $0.end) })
            .map { AdCopyLink(start: $0.start, end: $0.end) }
    }

    // MARK: - Barrier derivation (the positive content-resumed half)

    /// Project FM scan rows and the lexical hit stream into positive
    /// content-resumed barriers.
    ///
    /// TWO ADMISSIBLE SOURCES, both affirmative:
    ///
    ///  • An FM scan row whose `disposition == .noAds` with `status == .success`
    ///    over `transcriptQuality == .good`. The classifier was asked about that
    ///    window and ANSWERED that it holds no ads. The quality condition is the
    ///    same one `AdDetectionService.applyFMSuppression` uses to lift such a
    ///    row to `CertaintyBand.moderate`, so "an FM noAds we are willing to act
    ///    on" has one definition in this codebase. `.uncertain` and `.abstain`
    ///    rows are NOT barriers — an abstention is an absence, and absences are
    ///    exactly what must not become evidence here.
    ///
    ///  • An explicit spoken return marker (`.transitionMarker`): the host
    ///    saying the break is over. Same cue `TimeBoundaryResolver` scores as
    ///    `explicitReturnMarker`. Widened to `returnMarkerBarrierRadius` on each
    ///    side because the hit is an interpolated instant, not an interval, and a
    ///    zero-width barrier cannot block anything.
    ///
    ///  • A host-to-audience SHOW-BOUNDARY phrase (``showAddressPatterns``) —
    ///    "thanks for tuning in", "see you tomorrow", "we'll be right back". Added
    ///    after an audit caught this pass claiming a sign-off as ad; see that
    ///    property's note for the case.
    ///
    /// Metadata-origin lexical hits are excluded: a feed-description phrase is
    /// not an utterance at a time, so it cannot mark where content resumed.
    static func contentBarriers(
        semanticScanResults: [SemanticScanResult],
        lexicalHits: [LexicalHit],
        chunks: [TranscriptChunk] = []
    ) -> [ContentBarrier] {
        var barriers: [ContentBarrier] = []

        for row in semanticScanResults {
            guard row.disposition == .noAds,
                  row.status == .success,
                  row.transcriptQuality == .good,
                  row.windowStartTime.isFinite,
                  row.windowEndTime.isFinite,
                  row.windowEndTime > row.windowStartTime else {
                continue
            }
            barriers.append(
                ContentBarrier(start: row.windowStartTime, end: row.windowEndTime)
            )
        }

        for hit in lexicalHits {
            guard hit.category == .transitionMarker,
                  !hit.isMetadataOrigin,
                  !hit.isNegativePattern,
                  hit.startTime.isFinite,
                  hit.endTime.isFinite,
                  hit.endTime >= hit.startTime else {
                continue
            }
            barriers.append(
                ContentBarrier(
                    start: hit.startTime - returnMarkerBarrierRadius,
                    end: hit.endTime + returnMarkerBarrierRadius
                )
            )
        }

        for chunk in chunks {
            guard chunk.startTime.isFinite,
                  chunk.endTime.isFinite,
                  chunk.endTime > chunk.startTime,
                  !chunk.normalizedText.isEmpty else {
                continue
            }
            let text = chunk.normalizedText
            let range = NSRange(location: 0, length: (text as NSString).length)
            let matched = compiledShowAddressPatterns.contains {
                $0.firstMatch(in: text, range: range) != nil
            }
            if matched {
                barriers.append(ContentBarrier(start: chunk.startTime, end: chunk.endTime))
            }
        }

        let merged = unionIntervals(barriers.map { (start: $0.start, end: $0.end) })
        return merged.map { ContentBarrier(start: $0.start, end: $0.end) }
    }

    /// Host-to-audience SHOW-BOUNDARY phrases: the host speaking to the listener
    /// about the programme rather than selling anything. An affirmative statement
    /// that this second is show, which is why it belongs with the FM `noAds`
    /// verdict rather than in the ad-evidence set.
    ///
    /// FOUND BY AUDIT, not by imagination. The first calibrated run of this pass
    /// claimed `themove-2026-07-15` 2964.1–2987.1 as ad. Reading the transcript
    /// for those seconds: "Wow, we were all over the place today. I'm proud of us
    /// … Thanks for tuning in everybody. See you tomorrow." — the host's sign-off,
    /// sitting between a Ventum sponsor CTA the pipeline had confirmed and the
    /// post-roll pod, and the chain bridged straight across it. That was ~9 s of
    /// SHOW claimed as ad: the exact failure this bead must not ship.
    ///
    /// "we'll be right back" is here too. It is a GO-TO-BREAK marker rather than a
    /// return, but a barrier is a WALL, and this phrase sits precisely on the
    /// show/pod boundary — which is the only place it needs to hold.
    ///
    /// FAILS CLOSED IN THE RIGHT DIRECTION: if one of these fires inside a
    /// cross-promo ("thanks for listening to …"), the chain stops early and we
    /// lose recall. Losing an ad is the cheap error; eating the show is not.
    static let showAddressPatterns: [String] = [
        #"\bthanks for (tuning in|listening|joining us)\b"#,
        #"\bthank you for (tuning in|listening|joining us)\b"#,
        #"\bsee you (tomorrow|next (week|time|episode)|then)\b"#,
        #"\bwe.ll see you (tomorrow|next (week|time)|then)\b"#,
        #"\bthat.s (it|all) for (today|now|this (week|episode))\b"#,
        #"\buntil next time\b"#,
        #"\bwe.ll be right back\b"#,
        #"\bwe.re back\b"#,
        #"\bwelcome back to\b"#,
        #"\byou.re listening to\b"#
    ]

    private static let compiledShowAddressPatterns: [NSRegularExpression] =
        showAddressPatterns.compactMap {
            try? NSRegularExpression(pattern: $0, options: [.caseInsensitive])
        }

    /// Half-width (seconds) given to an explicit-return-marker barrier. A
    /// lexical hit's time is interpolated within its transcript chunk, so it
    /// carries a second or two of slop; a barrier narrower than that slop could
    /// be stepped over. Small enough that it cannot wall off a real neighbouring
    /// creative on its own.
    static let returnMarkerBarrierRadius = 2.0

    // MARK: - Compose

    /// Compose mark-only continuation windows for the ad-pod neighbours of the
    /// asset's confirmed ad windows.
    ///
    /// - Parameters:
    ///   - existingWindows: every persisted/fused window for the asset. Confirmed
    ///     non-user rows seed chains; all visible rows are used for dedupe.
    ///   - adCopyLinks: positive ad-copy evidence regions (see
    ///     ``adCopyLinks(hits:analysisAssetId:builder:config:)``).
    ///   - contentBarriers: positive content-resumed regions (see
    ///     ``contentBarriers(semanticScanResults:lexicalHits:)``).
    ///   - protectedRegions: time ranges the listener defined by hand. The walk
    ///     refuses to cross one. Must be supplied by the caller: persisted
    ///     `userMarked` rows may not be in `existingWindows`.
    ///   - episodeDuration: clamps every emitted mark to real audio. `<= 0`
    ///     means unknown and applies no clamp.
    /// - Returns: new mark-only `AdWindow`s covering recovered pod material.
    ///   Empty when nothing qualifies. Never returns a modified copy of an input
    ///   window.
    static func compose(
        existingWindows: [AdWindow],
        adCopyLinks: [AdCopyLink],
        contentBarriers: [ContentBarrier],
        protectedRegions: [(start: Double, end: Double)],
        episodeDuration: Double,
        analysisAssetId: String,
        config: Configuration = .default
    ) -> [AdWindow] {
        guard config.maxLinkGapSeconds.isFinite,
              config.maxLinkGapSeconds > 0,
              config.maxExtensionSecondsPerSide.isFinite,
              config.maxExtensionSecondsPerSide > 0 else {
            return []
        }
        let links = mergeLinks(adCopyLinks.filter {
            $0.start.isFinite && $0.end.isFinite && $0.end > $0.start
        })
        guard !links.isEmpty else { return [] }

        let barriers = contentBarriers.filter {
            $0.start.isFinite && $0.end.isFinite && $0.end > $0.start
        }
        // A degenerate protected region protects nothing — otherwise one bad row
        // becomes a permanent global veto (playhead-lc4c).
        let protected = protectedRegions.filter {
            $0.start.isFinite && $0.end.isFinite && $0.end > $0.start
        }
        let walls = barriers.map { (start: $0.start, end: $0.end) }
            + protected.map { (start: $0.start, end: $0.end) }

        let seeds = existingWindows.filter(isSeed)
        guard !seeds.isEmpty else { return [] }

        // Material the listener can ALREADY see, which the recovered span is
        // reduced against so this pass only ever adds genuinely-uncovered audio.
        //
        // Our OWN prior rows are excluded deliberately (mirroring
        // `SpecialistMarkComposer`'s dedupe): including them would make a re-run
        // find zero residue, emit nothing, and therefore RETIRE the rows it wrote
        // last time — a row that flickers in and out on every backfill.
        // Idempotency rides on content-addressed ids over identical residues, not
        // on self-suppression. `isOwnRow` tests BOTH provenance fields; see its
        // note for the accepted-banner row that a version-only test mis-claimed.
        let visible: [(start: Double, end: Double)] = existingWindows
            .filter {
                visibleDecisionStates.contains($0.decisionState)
                    && !isOwnRow($0)
                    && $0.startTime.isFinite
                    && $0.endTime.isFinite
                    && $0.endTime > $0.startTime
            }
            .map { (start: $0.startTime, end: $0.endTime) }

        var spans: [(start: Double, end: Double, confidence: Double)] = []
        for seed in seeds {
            let trailing = walkForward(from: seed, links: links, walls: walls, config: config)
            if let end = trailing, end > seed.endTime {
                spans.append((start: seed.endTime, end: end, confidence: seed.confidence))
            }
            let leading = walkBackward(from: seed, links: links, walls: walls, config: config)
            if let start = leading, start < seed.startTime {
                spans.append((start: start, end: seed.startTime, confidence: seed.confidence))
            }
        }
        guard !spans.isEmpty else { return [] }

        // Clamp to real audio, UNION the recovered spans, then reduce each to the
        // material NOTHING already covers.
        //
        // The union comes first because several seeds in one pod propose
        // overlapping spans — a seed either side of a hole both reach across it —
        // and subtracting per-span would emit two nearly-identical rows over the
        // same audio, i.e. two banners for one ad. Unioning makes the output
        // DISJOINT by construction.
        //
        // Emitting the residue, rather than dropping a mostly-covered span,
        // is what makes this pass move the quantity the bead measures: the
        // UNCOVERED runs inside a pod. It also means the pass can never
        // double-count — a hole between two already-detected creatives yields a
        // mark for the hole, not a second row over the creatives.
        let upperBound = episodeDuration.isFinite && episodeDuration > 0
            ? episodeDuration
            : Double.greatestFiniteMagnitude
        let clamped = spans.compactMap { span -> (start: Double, end: Double)? in
            let start = max(span.start, 0)
            let end = min(span.end, upperBound)
            return end > start ? (start: start, end: end) : nil
        }

        var marks: [AdWindow] = []
        for recovered in unionIntervals(clamped) {
            // Presence confidence for this stretch: the strongest seed that
            // reached it. Never averaged down, never raised above the ceiling.
            let confidence = spans
                .filter { $0.start < recovered.end && $0.end > recovered.start }
                .map(\.confidence)
                .max() ?? 0
            for residue in subtract(covered: visible, from: recovered) {
                guard residue.end - residue.start >= config.minMarkDurationSeconds else {
                    continue
                }
                marks.append(
                    makeMark(
                        start: residue.start,
                        end: residue.end,
                        confidence: min(confidence, config.markConfidenceCeiling),
                        analysisAssetId: analysisAssetId
                    )
                )
            }
        }
        return marks
    }

    // MARK: - Seed selection

    /// Boundary states that belong to the LISTENER rather than to a detector.
    ///
    /// Deliberately its own set rather than reusing
    /// `AdDetectionService.reconcileProtectedBoundaryStates`, which was the first
    /// implementation and was wrong in both directions. That set also contains
    /// `dayZeroRediffByteExact` — not a user row at all, but the BYTE-EXACT
    /// rediff pod boundary, i.e. the highest-certainty ad window the pipeline
    /// produces and the most obviously correct thing to chain a pod walk off.
    /// Reusing it silently refused to seed from exactly the best seed. And it
    /// omits nothing a listener owns, so the two concerns simply are not the same
    /// set.
    static let userOwnedBoundaryStates: Set<String> = [
        "userMarked",
        "userConfirmedSuggested",
        "correctionReplay"
    ]

    /// A window may seed a continuation chain iff it is a CONFIRMED detector
    /// verdict with sane geometry and is not the listener's own row.
    ///
    /// User-owned rows are excluded deliberately. A manual mark is the
    /// highest-fidelity statement about the span the listener drew — it says
    /// nothing about the neighbouring audio, and the listener who marked one
    /// creative has already told us what they wanted marked. Deriving more marks
    /// off their boundary would put a guess in a place they curated.
    static func isSeed(_ window: AdWindow) -> Bool {
        seedDecisionStates.contains(window.decisionState)
            && !userOwnedBoundaryStates.contains(window.boundaryState)
            && !isOwnRow(window)
            && window.startTime.isFinite
            && window.endTime.isFinite
            && window.endTime > window.startTime
    }

    /// `true` when this row is one THIS pass minted.
    ///
    /// Both fields, not just `detectorVersion`: when the listener ACCEPTS a
    /// continuation banner the orchestrator mints a promoted row that inherits our
    /// `detectorVersion` but carries `boundaryState == "userConfirmedSuggested"`.
    /// A `detectorVersion`-only test treated that promoted row as ours and
    /// excluded it from the coverage set, so the next backfill re-claimed the
    /// span the listener had already resolved.
    static func isOwnRow(_ window: AdWindow) -> Bool {
        window.detectorVersion == detectorVersion && window.boundaryState == boundaryState
    }

    // MARK: - The chain walk

    /// Walk the ad-copy chain forward from the seed's trailing edge, returning
    /// the new trailing edge (or `nil` when the chain never starts).
    ///
    /// Each step requires a link that begins within `maxLinkGapSeconds` of the
    /// current edge and reaches past it. The corridor `[edge, link.end)` must
    /// cross no wall (content barrier or protected region). The returned edge is
    /// always some link's `end`: the walk NEVER lands anywhere that positive ad
    /// evidence did not put it.
    private static func walkForward(
        from seed: AdWindow,
        links: [AdCopyLink],
        walls: [(start: Double, end: Double)],
        config: Configuration
    ) -> Double? {
        var edge = seed.endTime
        var moved = false
        // Bounded by the link count: every accepted step consumes at least one
        // link (edge strictly increases past that link's end), so this cannot
        // spin. The explicit budget keeps that true even if `links` is huge.
        for _ in 0..<links.count {
            let reachable = links.filter {
                $0.end > edge && $0.start <= edge + config.maxLinkGapSeconds
            }
            guard let next = reachable.min(by: { lhs, rhs in
                if lhs.start != rhs.start { return lhs.start < rhs.start }
                return lhs.end < rhs.end
            }) else { break }
            guard next.end - seed.endTime <= config.maxExtensionSecondsPerSide else { break }
            guard !intersectsWall(start: edge, end: next.end, walls: walls) else { break }
            edge = next.end
            moved = true
        }
        return moved ? edge : nil
    }

    /// Mirror of ``walkForward(from:links:walls:config:)`` for the leading edge.
    private static func walkBackward(
        from seed: AdWindow,
        links: [AdCopyLink],
        walls: [(start: Double, end: Double)],
        config: Configuration
    ) -> Double? {
        var edge = seed.startTime
        var moved = false
        for _ in 0..<links.count {
            let reachable = links.filter {
                $0.start < edge && $0.end >= edge - config.maxLinkGapSeconds
            }
            guard let next = reachable.max(by: { lhs, rhs in
                if lhs.end != rhs.end { return lhs.end < rhs.end }
                return lhs.start < rhs.start
            }) else { break }
            guard seed.startTime - next.start <= config.maxExtensionSecondsPerSide else { break }
            guard !intersectsWall(start: next.start, end: edge, walls: walls) else { break }
            edge = next.start
            moved = true
        }
        return moved ? edge : nil
    }

    /// `true` when any wall overlaps the half-open corridor `[start, end)`.
    ///
    /// Half-open on purpose: a barrier that begins exactly where the corridor
    /// ends is the wall we stopped AT, not one we crossed, and a barrier that
    /// ends exactly where the corridor begins is already behind us.
    private static func intersectsWall(
        start: Double,
        end: Double,
        walls: [(start: Double, end: Double)]
    ) -> Bool {
        guard end > start else { return false }
        return walls.contains { $0.start < end && $0.end > start }
    }

    // MARK: - Interval helpers

    /// `span` minus the union of `covered`, as an ascending list of gaps. The
    /// residue is what nothing already claims — the "uncovered run" this bead
    /// measures, computed the same way the offline measurement does.
    static func subtract(
        covered: [(start: Double, end: Double)],
        from span: (start: Double, end: Double)
    ) -> [(start: Double, end: Double)] {
        guard span.end > span.start else { return [] }
        let clipped = covered.compactMap { iv -> (start: Double, end: Double)? in
            let start = max(span.start, iv.start)
            let end = min(span.end, iv.end)
            return end > start ? (start: start, end: end) : nil
        }
        var result: [(start: Double, end: Double)] = []
        var cursor = span.start
        for block in unionIntervals(clipped) {
            if block.start > cursor { result.append((start: cursor, end: block.start)) }
            cursor = max(cursor, block.end)
        }
        if cursor < span.end { result.append((start: cursor, end: span.end)) }
        return result
    }

    /// Sort and union `[start, end)` intervals into a disjoint, ascending set.
    /// Touching intervals merge — for a chain walk, "adjacent" and "overlapping"
    /// are the same thing.
    static func unionIntervals(
        _ intervals: [(start: Double, end: Double)]
    ) -> [(start: Double, end: Double)] {
        let sorted = intervals
            .filter { $0.start.isFinite && $0.end.isFinite && $0.end > $0.start }
            .sorted { lhs, rhs in
                if lhs.start != rhs.start { return lhs.start < rhs.start }
                return lhs.end < rhs.end
            }
        var result: [(start: Double, end: Double)] = []
        for interval in sorted {
            if let last = result.last, interval.start <= last.end {
                result[result.count - 1] = (
                    start: last.start,
                    end: max(last.end, interval.end)
                )
            } else {
                result.append(interval)
            }
        }
        return result
    }

    // MARK: - Emit

    /// Build the content-addressed mark-only `AdWindow` for a recovered span.
    ///
    /// Every authority field is a hard-coded literal, not a derivation: the gate
    /// is `.markOnly`, the state is `.candidate`, and both edge anchors are
    /// `.unanchored`. There is no code path in this file that can produce
    /// anything else, which is what makes "a continuation can never auto-skip"
    /// a property rather than a promise.
    static func makeMark(
        start: Double,
        end: Double,
        confidence: Double,
        analysisAssetId: String
    ) -> AdWindow {
        AdWindow(
            id: markId(analysisAssetId: analysisAssetId, start: start, end: end),
            analysisAssetId: analysisAssetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: boundaryState,
            // NEVER confirmed/applied.
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: detectorVersion,
            // We know a pod continues; we do NOT know whose ad this is.
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: metadataSource,
            // nil → generic no-hallucination banner copy.
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            // ALWAYS markOnly — hard-coded literal, never a policy switch.
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            // Belt+suspenders: under playhead-2350 an unanchored edge auto-skips
            // nothing, so even a future edge policy running over these rows
            // cannot promote recovered pod material to a silent skip.
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    /// Content-addressed id: `podcont-<16 hex>` over
    /// `asset=…|version=pod-continuation-v1|start=…|end=…`. An identical recompose
    /// mints the identical id, so the version-scoped reconcile retires nothing and
    /// the store's INSERT-OR-REPLACE is a true no-op (idempotency by construction).
    static func markId(analysisAssetId: String, start: Double, end: Double) -> String {
        let canonical =
            "asset=\(analysisAssetId)|version=\(detectorVersion)|start=\(start)|end=\(end)"
        let digest = SHA256.hash(data: Data(canonical.utf8))
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return "podcont-\(hex.prefix(16))"
    }
}
