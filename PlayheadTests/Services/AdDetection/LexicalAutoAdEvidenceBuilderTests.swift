// LexicalAutoAdEvidenceBuilderTests.swift
// playhead-xsdz.1: Hermetic unit tests for the high-precision lexical
// auto-ad rule.
//
// These tests are FULLY hermetic (no FM, no audio, no corpus) so they run on
// every Cmd-U / PlayheadFastTests pass. They prove:
//   1. The rule FIRES on a strong sponsor + promo-code / URL-CTA combo inside
//      a tight window, emitting a `.lexicalAutoAd` entry.
//   2. The rule is SUPPRESSED by every negative-evidence guardrail
//      (show-owned-domain negative pattern, news/review context, metadata-
//      only legs, wide separation, a single signal alone).
//   3. The emitted weight, fused through `BackfillEvidenceFusion` +
//      `DecisionMapper`, clears the `.lexicalAutoAdQualified` auto-skip
//      threshold (0.50) while a non-firing span does NOT — i.e. the rule
//      genuinely drives the live decision and is precision-controlled.

import Foundation
import Testing
@testable import Playhead

@Suite("LexicalAutoAdEvidenceBuilder")
struct LexicalAutoAdEvidenceBuilderTests {

    // MARK: - Helpers

    private func makeSpan(
        assetId: String = "asset-1",
        startTime: Double = 0.0,
        endTime: Double = 60.0,
        anchorProvenance: [AnchorRef] = []
    ) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 1, lastAtomOrdinal: 2),
            assetId: assetId,
            firstAtomOrdinal: 1,
            lastAtomOrdinal: 2,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: anchorProvenance
        )
    }

    private func hit(
        _ category: LexicalPatternCategory,
        _ text: String,
        at time: Double,
        weight: Double = 1.0,
        isMetadataOrigin: Bool = false,
        isNegativePattern: Bool = false
    ) -> LexicalHit {
        LexicalHit(
            category: category,
            matchedText: text,
            startTime: time,
            endTime: time + 0.5,
            weight: weight,
            isMetadataOrigin: isMetadataOrigin,
            isNegativePattern: isNegativePattern
        )
    }

    private let builder = LexicalAutoAdEvidenceBuilder()

    // MARK: - Positive: rule fires on a strong combo

    @Test("Fires on sponsor + promo-code within the tight window")
    func firesOnSponsorPlusPromo() {
        let span = makeSpan()
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            hit(.promoCode, "use code save20", at: 14.0),
        ]
        let entries = builder.buildEntries(hits: hits, for: span)
        #expect(entries.count == 1)
        #expect(entries.first?.source == .lexicalAutoAd)
        #expect((entries.first?.weight ?? 0) > 0.5)
    }

    @Test("Fires on sponsor + url-CTA within the tight window")
    func firesOnSponsorPlusUrl() {
        let span = makeSpan()
        let hits = [
            hit(.sponsor, "sponsored by", at: 30.0),
            hit(.urlCTA, "visit acme com", at: 36.0, weight: 0.8),
        ]
        let entries = builder.buildEntries(hits: hits, for: span)
        #expect(entries.count == 1)
        #expect(entries.first?.source == .lexicalAutoAd)
    }

    @Test("Detail records the two contributing categories")
    func detailRecordsCategories() {
        let span = makeSpan()
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            hit(.promoCode, "promo code abc", at: 12.0),
        ]
        let entries = builder.buildEntries(hits: hits, for: span)
        guard case .lexical(let cats)? = entries.first?.detail else {
            Issue.record("expected .lexical detail")
            return
        }
        #expect(cats.contains("sponsor"))
        #expect(cats.contains("promoCode"))
    }

    // MARK: - Negative: single signal is not enough

    @Test("Does NOT fire on a lone sponsor disclosure")
    func noFireOnLoneSponsor() {
        let span = makeSpan()
        let hits = [hit(.sponsor, "brought to you by", at: 10.0)]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    @Test("Does NOT fire on a lone promo code (no sponsor leg)")
    func noFireOnLonePromo() {
        let span = makeSpan()
        let hits = [hit(.promoCode, "use code save20", at: 10.0)]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    @Test("Does NOT fire on a lone URL CTA (no sponsor leg)")
    func noFireOnLoneUrl() {
        let span = makeSpan()
        let hits = [hit(.urlCTA, "visit acme com", at: 10.0, weight: 0.8)]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    @Test("Does NOT fire on purchase-language alone (not a sponsor+CTA combo)")
    func noFireOnPurchaseLanguageAlone() {
        let span = makeSpan()
        let hits = [
            hit(.purchaseLanguage, "free trial", at: 10.0, weight: 0.9),
            hit(.purchaseLanguage, "money back guarantee", at: 12.0, weight: 0.9),
        ]
        // purchaseLanguage is neither a sponsor nor a promo/URL leg.
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    @Test("Does NOT fire on transition markers")
    func noFireOnTransitionMarkers() {
        let span = makeSpan()
        let hits = [
            hit(.transitionMarker, "back to the show", at: 10.0, weight: 0.3),
            hit(.transitionMarker, "anyway", at: 12.0, weight: 0.3),
        ]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    // MARK: - Negative: co-occurrence window

    @Test("Does NOT fire when sponsor and CTA are too far apart")
    func noFireWhenSeparatedBeyondWindow() {
        let span = makeSpan(startTime: 0.0, endTime: 120.0)
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            // 40s later — well beyond the 25s default window.
            hit(.promoCode, "use code save20", at: 50.0),
        ]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    @Test("Fires when separation is exactly at the window edge")
    func firesAtWindowEdge() {
        let span = makeSpan(startTime: 0.0, endTime: 120.0)
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            hit(.promoCode, "use code save20", at: 35.0), // exactly 25s gap
        ]
        #expect(builder.buildEntries(hits: hits, for: span).count == 1)
    }

    // MARK: - Negative-evidence guardrails

    @Test("Suppressed by a show-owned-domain negative pattern")
    func suppressedByNegativePattern() {
        let span = makeSpan()
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            hit(.promoCode, "use code save20", at: 13.0),
            // Show plugging its OWN domain — negative pattern.
            hit(.urlCTA, "our show com", at: 12.0, weight: 0.8, isNegativePattern: true),
        ]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    // Reach note (see builder file header SCOPE LIMIT): the editorial-cue
    // guardrail inspects each hit's own `matchedText`, not the surrounding
    // prose. These tests therefore embed the cue INSIDE a hit's matched text
    // — which is exactly what a multi-word per-show sponsor-lexicon / metadata
    // phrase produces in production. The companion `editorialCueOnlyInNarration…`
    // test pins the negative side of that contract so the limit is explicit,
    // not accidental.
    @Test("Suppressed when an editorial cue is embedded in the sponsor hit's matched text")
    func suppressedByNewsContext() {
        let span = makeSpan()
        let hits = [
            // The sponsor-shaped phrase is actually editorial framing.
            hit(.sponsor, "according to a new lawsuit", at: 10.0),
            hit(.urlCTA, "visit acme com", at: 13.0, weight: 0.8),
        ]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    @Test("Suppressed when an editorial cue is embedded in a nearby hit's matched text")
    func suppressedByNearbyEditorialCue() {
        let span = makeSpan()
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            hit(.promoCode, "use code save20", at: 13.0),
            // A nearby hit whose OWN matched text carries an editorial cue.
            hit(.urlCTA, "researchers found acme com", at: 16.0, weight: 0.8),
        ]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    @Test("Editorial cue confined to narration (not in any hit's matched text) does NOT suppress — precision rests on the co-occurrence bar, not this guardrail")
    func editorialCueOnlyInNarrationDoesNotSuppress() {
        // Pins the SCOPE LIMIT honestly: when the editorial frame lives only
        // in the surrounding transcript prose — i.e. NOT inside any hit's
        // matched text — the hit-stream guardrail cannot see it, so the combo
        // still fires. This is acceptable precisely because a real sponsor
        // disclosure ("brought to you by") co-occurring with a promo code is
        // an ad regardless of nearby editorial words; the strong co-occurrence
        // requirement is the load-bearing precision control. If a future bead
        // threads transcript text into the builder to widen the guardrail,
        // THIS expectation is the one that should consciously change.
        let span = makeSpan()
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            hit(.promoCode, "use code save20", at: 13.0),
            // Built-in regex matched text never embeds an editorial cue.
            hit(.urlCTA, "acme com", at: 16.0, weight: 0.8),
        ]
        // No hit's matchedText contains a cue → guardrail does not engage.
        #expect(builder.buildEntries(hits: hits, for: span).count == 1)
    }

    // MARK: - Metadata-origin supplementary rule

    @Test("Metadata-origin hits cannot, alone, be the sponsor leg")
    func metadataOnlySponsorDoesNotTrigger() {
        let span = makeSpan()
        let hits = [
            // sponsor leg is metadata-origin (supplementary only)
            hit(.sponsor, "acme", at: 10.0, isMetadataOrigin: true),
            hit(.promoCode, "use code save20", at: 13.0),
        ]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    @Test("Metadata-origin hits cannot, alone, be the CTA leg")
    func metadataOnlyCtaDoesNotTrigger() {
        let span = makeSpan()
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            hit(.urlCTA, "acme com", at: 13.0, weight: 0.8, isMetadataOrigin: true),
        ]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    // MARK: - Span-overlap scoping

    @Test("Only considers hits overlapping the span interval")
    func ignoresHitsOutsideSpan() {
        // Span covers [100, 160]; the combo is entirely before it.
        let span = makeSpan(startTime: 100.0, endTime: 160.0)
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            hit(.promoCode, "use code save20", at: 13.0),
        ]
        #expect(builder.buildEntries(hits: hits, for: span).isEmpty)
    }

    @Test("Empty hit list yields no entry")
    func emptyHits() {
        #expect(builder.buildEntries(hits: [], for: makeSpan()).isEmpty)
    }

    // MARK: - End-to-end: fused decision clears the qualified threshold

    @Test("Fired entry drives a fused decision above the qualified auto-skip floor")
    func firedEntryClearsQualifiedThreshold() {
        let span = makeSpan() // no FM anchor → eligible for the qualified track
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            hit(.promoCode, "use code save20", at: 13.0),
        ]
        let autoAdEntries = builder.buildEntries(hits: hits, for: span)
        #expect(!autoAdEntries.isEmpty)

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0, // classifier says nothing — lexical-only
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            lexicalAutoAdEntries: autoAdEntries,
            mode: .full,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let mapper = DecisionMapper(span: span, ledger: ledger, config: FusionWeightConfig())
        let decision = mapper.map()

        // The span must be eligible AND clear the qualified track's floor
        // (0.50 default) — i.e. a confirmed combo skips on its own.
        #expect(decision.promotionTrack == .lexicalAutoAdQualified)
        #expect(decision.eligibilityGate == .eligible)
        let config = AdDetectionConfig.default
        let threshold = config.effectiveAutoSkipThreshold(for: decision.promotionTrack)
        #expect(decision.skipConfidence >= threshold)
        // But the standard 0.80 gate must remain UNREACHABLE for a lexical-
        // only span — precision guard: it only skips via the qualified track.
        #expect(decision.skipConfidence < config.autoSkipConfidenceThreshold)
    }

    @Test("A non-firing span stays on the standard track and below the auto-skip floor")
    func nonFiringSpanStaysStandard() {
        let span = makeSpan()
        // A lone sponsor mention — the rule does not fire.
        let hits = [hit(.sponsor, "brought to you by", at: 10.0)]
        let autoAdEntries = builder.buildEntries(hits: hits, for: span)
        #expect(autoAdEntries.isEmpty)

        // Even feeding the (modest) raw lexical entry, the standard gate is
        // unreachable: lexicalCap = 0.20 << 0.80.
        let lexicalEntry = EvidenceLedgerEntry(
            source: .lexical,
            weight: 0.20,
            detail: .lexical(matchedCategories: ["sponsor"])
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [lexicalEntry],
            acousticEntries: [],
            catalogEntries: [],
            lexicalAutoAdEntries: autoAdEntries,
            mode: .full,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let mapper = DecisionMapper(span: span, ledger: ledger, config: FusionWeightConfig())
        let decision = mapper.map()

        #expect(decision.promotionTrack == .standard)
        let config = AdDetectionConfig.default
        let threshold = config.effectiveAutoSkipThreshold(for: decision.promotionTrack)
        #expect(decision.skipConfidence < threshold)
    }

    @Test("FM-anchored span with a combo stays on the standard track (M-2 carve-out)")
    func fmAnchoredStaysStandard() {
        // A span carrying an FM-class anchor has independent FM evidence and
        // must clear the standard gate on its own merits, not the lexical
        // qualified track.
        let span = makeSpan(
            anchorProvenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.9)]
        )
        let hits = [
            hit(.sponsor, "brought to you by", at: 10.0),
            hit(.promoCode, "use code save20", at: 13.0),
        ]
        let autoAdEntries = builder.buildEntries(hits: hits, for: span)
        #expect(!autoAdEntries.isEmpty) // builder still fires...

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            lexicalAutoAdEntries: autoAdEntries,
            mode: .full,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let mapper = DecisionMapper(span: span, ledger: ledger, config: FusionWeightConfig())
        let decision = mapper.map()
        // ...but the FM-anchored span does NOT take the lexical qualified track.
        #expect(decision.promotionTrack == .standard)
    }
}
