// AdBannerCopyTests.swift
// Verifies banner copy logic: confidence gating, advertiser/product surfacing,
// and generic fallback for weak evidence. Covers playhead-9yj acceptance criteria.

import XCTest
@testable import Playhead

@MainActor
final class AdBannerCopyTests: XCTestCase {

    // MARK: - Helpers

    private func makeBannerItem(
        advertiser: String? = nil,
        product: String? = nil,
        metadataConfidence: Double? = nil,
        metadataSource: String = "none",
        tier: AdBannerTier = .autoSkipped
    ) -> AdSkipBannerItem {
        AdSkipBannerItem(
            id: UUID().uuidString,
            windowId: "w-\(UUID().uuidString)",
            advertiser: advertiser,
            product: product,
            adStartTime: 120.0,
            adEndTime: 180.0,
            metadataConfidence: metadataConfidence,
            metadataSource: metadataSource,
            podcastId: "podcast-test",
            evidenceCatalogEntries: [],
            tier: tier
        )
    }

    // MARK: - Threshold constant

    func testMetadataConfidenceThresholdIs060() {
        XCTAssertEqual(
            AdBannerView.metadataConfidenceThreshold, 0.60,
            "metadataConfidenceThreshold must be hardcoded to 0.60"
        )
    }

    // MARK: - High confidence + strong evidence

    func testHighConfidenceWithAdvertiserAndProduct() {
        let item = makeBannerItem(
            advertiser: "Squarespace",
            product: "Build your website",
            metadataConfidence: 0.85,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Skipped")
        XCTAssertEqual(copy.advertiser, "Squarespace")
        XCTAssertEqual(copy.detail, "Build your website")
    }

    func testHighConfidenceWithAdvertiserNoProduct() {
        let item = makeBannerItem(
            advertiser: "BetterHelp",
            product: nil,
            metadataConfidence: 0.75,
            metadataSource: "fallback"
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Skipped")
        XCTAssertEqual(copy.advertiser, "BetterHelp")
        XCTAssertNil(copy.detail)
    }

    func testExactThresholdConfidenceSurfacesAdvertiser() {
        let item = makeBannerItem(
            advertiser: "Athletic Greens",
            product: nil,
            metadataConfidence: 0.60,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Skipped",
            "Confidence exactly at threshold (0.60) should surface advertiser")
        XCTAssertEqual(copy.advertiser, "Athletic Greens")
    }

    // MARK: - Low confidence fallback

    func testBelowThresholdFallsBackToGeneric() {
        let item = makeBannerItem(
            advertiser: "Maybe Corp",
            product: "Some product",
            metadataConfidence: 0.59,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Skipped sponsor segment",
            "Below-threshold confidence must fall back to generic copy")
        XCTAssertNil(copy.advertiser,
            "Advertiser must not be surfaced below confidence threshold")
        XCTAssertNil(copy.detail,
            "Product must not be surfaced below confidence threshold")
    }

    func testZeroConfidenceFallsBackToGeneric() {
        let item = makeBannerItem(
            advertiser: "Ghost Corp",
            metadataConfidence: 0.0,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Skipped sponsor segment")
        XCTAssertNil(copy.advertiser)
    }

    // MARK: - Missing evidence fallback

    func testNilConfidenceFallsBackToGeneric() {
        let item = makeBannerItem(
            advertiser: "Phantom Inc",
            metadataConfidence: nil,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Skipped sponsor segment")
        XCTAssertNil(copy.advertiser)
    }

    func testMetadataSourceNoneFallsBackToGeneric() {
        let item = makeBannerItem(
            advertiser: "Real Advertiser",
            product: "Real Product",
            metadataConfidence: 0.99,
            metadataSource: "none"
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Skipped sponsor segment",
            "metadataSource='none' must suppress advertiser even at high confidence")
        XCTAssertNil(copy.advertiser)
        XCTAssertNil(copy.detail)
    }

    func testNoAdvertiserFallsBackToGenericEvenWithHighConfidence() {
        let item = makeBannerItem(
            advertiser: nil,
            product: "Some product",
            metadataConfidence: 0.90,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Skipped sponsor segment",
            "Missing advertiser must produce generic copy regardless of confidence")
        XCTAssertNil(copy.advertiser)
        XCTAssertNil(copy.detail,
            "Product must not be surfaced without an advertiser")
    }

    func testNoMetadataAtAllFallsBackToGeneric() {
        let item = makeBannerItem(
            advertiser: nil,
            product: nil,
            metadataConfidence: nil,
            metadataSource: "none"
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Skipped sponsor segment")
        XCTAssertNil(copy.advertiser)
        XCTAssertNil(copy.detail)
    }

    // MARK: - Template-driven (no hallucination)

    func testCopyIsTemplateDriven() {
        // High confidence path: only prefix + advertiser + detail — no free-form text
        let item = makeBannerItem(
            advertiser: "HelloFresh",
            product: "Meal kits",
            metadataConfidence: 0.80,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)

        // The prefix is always the literal "Skipped", never a generated sentence.
        XCTAssertEqual(copy.prefix, "Skipped")
        // The advertiser is passed through verbatim, never rewritten.
        XCTAssertEqual(copy.advertiser, "HelloFresh")
        // The detail is passed through verbatim, never rewritten.
        XCTAssertEqual(copy.detail, "Meal kits")
    }

    // MARK: - Evidence Copy (playhead-vjxc)

    private func makeEntry(
        category: EvidenceCategory,
        text: String,
        ref: Int = 0,
        start: Double = 100,
        end: Double = 101
    ) -> EvidenceEntry {
        EvidenceEntry(
            evidenceRef: ref,
            category: category,
            matchedText: text,
            normalizedText: text.lowercased(),
            atomOrdinal: 0,
            startTime: start,
            endTime: end
        )
    }

    func testEvidenceLineForDisclosurePhraseQuotesText() {
        let line = AdBannerView.evidenceLine(
            for: makeEntry(category: .disclosurePhrase, text: "brought to you by")
        )
        XCTAssertEqual(line, "Sponsor disclosure: \u{201C}brought to you by\u{201D}")
    }

    func testEvidenceLineForUrlIsUnquoted() {
        let line = AdBannerView.evidenceLine(
            for: makeEntry(category: .url, text: "betterhelp.com/podcast")
        )
        XCTAssertEqual(line, "Sponsor link: betterhelp.com/podcast")
    }

    func testEvidenceLineForPromoCodeIsUnquoted() {
        let line = AdBannerView.evidenceLine(
            for: makeEntry(category: .promoCode, text: "use code SAVE10")
        )
        XCTAssertEqual(line, "Promo code: use code SAVE10")
    }

    func testEvidenceLineForCtaPhraseQuotesText() {
        let line = AdBannerView.evidenceLine(
            for: makeEntry(category: .ctaPhrase, text: "sign up today")
        )
        XCTAssertEqual(line, "Sponsor cue: \u{201C}sign up today\u{201D}")
    }

    func testEvidenceLineForBrandSpanIsUnquotedAndPreservesCasing() {
        let line = AdBannerView.evidenceLine(
            for: makeEntry(category: .brandSpan, text: "BetterHelp")
        )
        XCTAssertEqual(line, "Sponsor mention: BetterHelp")
    }

    func testEvidenceLinesEmptyWhenNoEntries() {
        XCTAssertEqual(AdBannerView.evidenceLines(for: []), [])
    }

    func testEvidenceLinesOrderedByPriority() {
        let entries = [
            makeEntry(category: .ctaPhrase, text: "sign up today"),
            makeEntry(category: .brandSpan, text: "BetterHelp"),
            makeEntry(category: .promoCode, text: "code SAVE10"),
            makeEntry(category: .disclosurePhrase, text: "sponsored by"),
            makeEntry(category: .url, text: "betterhelp.com"),
        ]
        let lines = AdBannerView.evidenceLines(for: entries)
        // Limit is 3 — we expect promoCode, url, disclosure (in that order).
        XCTAssertEqual(lines.count, 3)
        XCTAssertTrue(lines[0].hasPrefix("Promo code:"), "Got: \(lines[0])")
        XCTAssertTrue(lines[1].hasPrefix("Sponsor link:"), "Got: \(lines[1])")
        XCTAssertTrue(lines[2].hasPrefix("Sponsor disclosure:"), "Got: \(lines[2])")
    }

    func testEvidenceLinesRespectsLineLimit() {
        let entries = (0..<10).map { i in
            makeEntry(
                category: .promoCode,
                text: "code C\(i)",
                ref: i,
                start: Double(i),
                end: Double(i) + 0.5
            )
        }
        let lines = AdBannerView.evidenceLines(for: entries)
        XCTAssertEqual(lines.count, AdBannerView.evidenceLineLimit)
    }

    func testEvidenceLinesDeduplicatesCaseInsensitive() {
        let entries = [
            makeEntry(category: .brandSpan, text: "BetterHelp", ref: 0),
            makeEntry(category: .brandSpan, text: "betterhelp", ref: 1),
            makeEntry(category: .brandSpan, text: "BETTERHELP", ref: 2),
        ]
        let lines = AdBannerView.evidenceLines(for: entries)
        XCTAssertEqual(lines.count, 1, "Three case variants of one brand should collapse to one line")
    }

    func testEvidenceLineLimitIs3() {
        // Locked-in constant: 3 keeps the banner glanceable.
        XCTAssertEqual(AdBannerView.evidenceLineLimit, 3)
    }

    func testEvidenceLineTrimsWhitespace() {
        let line = AdBannerView.evidenceLine(
            for: makeEntry(category: .url, text: "  betterhelp.com  ")
        )
        XCTAssertEqual(line, "Sponsor link: betterhelp.com")
    }

    // MARK: - Suggest Tier Copy (playhead-gtt9.23)
    //
    // Suggest-tier banners ask the user to confirm a skip that has NOT
    // happened. Their voice is calm and evidence-bound: "Sounds like a
    // sponsor break." — never a quantified probability, never "X%
    // confidence." Per `feedback_peace_of_mind_not_metrics`, the
    // suggest copy describes what was heard, not how sure we are.

    func testSuggestTierUsesSoundsLikePrefix() {
        let item = makeBannerItem(
            advertiser: "Squarespace",
            product: "Build your website",
            metadataConfidence: 0.85,
            metadataSource: "foundationModels",
            tier: .suggest
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Sounds like a sponsor break",
            "Suggest tier must use the calm declarative prefix, never the past-tense 'Skipped'")
        XCTAssertEqual(copy.advertiser, "Squarespace",
            "Suggest tier still surfaces a known advertiser when evidence is strong")
        XCTAssertEqual(copy.detail, "Build your website")
    }

    func testSuggestTierWithWeakEvidenceFallsBackToCalmGeneric() {
        let item = makeBannerItem(
            advertiser: "Maybe Corp",
            product: "Some product",
            metadataConfidence: 0.40,
            metadataSource: "foundationModels",
            tier: .suggest
        )
        let copy = AdBannerView.bannerCopy(for: item)

        // Below the metadata confidence threshold the advertiser is not
        // surfaced — same evidence rule as the autoSkipped tier — but
        // the *prefix* must remain the suggest-tier voice, not collapse
        // back into "Skipped sponsor segment".
        XCTAssertEqual(copy.prefix, "Sounds like a sponsor break",
            "Suggest tier with weak evidence must still use the suggest prefix, not the auto-skipped fallback")
        XCTAssertNil(copy.advertiser,
            "Below-threshold confidence must not surface advertiser, even on suggest tier")
        XCTAssertNil(copy.detail)
    }

    func testSuggestTierContainsNoQuantifiedLanguage() {
        // Belt-and-suspenders against future drift: every suggest copy
        // we produce must avoid percent signs, the word "confidence",
        // and any digit cluster that looks like a probability. This is
        // a content-policy test — it'll fail loudly if someone slips
        // "73% sure" or "confidence: 0.45" into the prefix later.
        let cases: [AdSkipBannerItem] = [
            makeBannerItem(tier: .suggest),
            makeBannerItem(advertiser: "BetterHelp", metadataConfidence: 0.70,
                           metadataSource: "foundationModels", tier: .suggest),
            makeBannerItem(advertiser: nil, metadataConfidence: 0.20,
                           metadataSource: "foundationModels", tier: .suggest),
        ]
        for item in cases {
            let copy = AdBannerView.bannerCopy(for: item)
            let blob = [copy.prefix, copy.advertiser, copy.detail]
                .compactMap { $0 }
                .joined(separator: " ")
                .lowercased()
            XCTAssertFalse(blob.contains("%"),
                "Suggest copy must not contain '%' (got: \(blob))")
            XCTAssertFalse(blob.contains("confidence"),
                "Suggest copy must not contain 'confidence' (got: \(blob))")
            XCTAssertNil(blob.range(of: #"\b\d{1,3}\s*%"#, options: .regularExpression),
                "Suggest copy must not contain percent-style numbers (got: \(blob))")
        }
    }

    func testAutoSkippedTierStillUsesSkippedPrefix() {
        // Regression guard: introducing the suggest tier must not
        // change the existing auto-skipped voice. Same fixture as the
        // playhead-9yj copy tests, asserted with an explicit
        // tier=.autoSkipped to pin the contract.
        let item = makeBannerItem(
            advertiser: "Squarespace",
            metadataConfidence: 0.85,
            metadataSource: "foundationModels",
            tier: .autoSkipped
        )
        let copy = AdBannerView.bannerCopy(for: item)

        XCTAssertEqual(copy.prefix, "Skipped",
            "Auto-skipped tier voice must remain 'Skipped' after the suggest tier landed")
        XCTAssertEqual(copy.advertiser, "Squarespace")
    }

    // MARK: - Tier-aware dwell (playhead-gtt9.23)

    func testSuggestTierHasLongerDwellThanAutoSkipped() {
        // Suggest banners ask the user a question — they need a beat
        // longer to read and decide than auto-skipped banners (which
        // are an after-the-fact notification). The exact ratio is not
        // pinned, but suggest must be strictly longer than auto-skipped.
        let suggestDwell = AdBannerQueue.dwellSeconds(for: .suggest)
        let autoSkippedDwell = AdBannerQueue.dwellSeconds(for: .autoSkipped)
        XCTAssertGreaterThan(suggestDwell, autoSkippedDwell,
            "Suggest dwell (\(suggestDwell)s) must exceed auto-skipped dwell (\(autoSkippedDwell)s)")
    }

    func testAutoSkippedDwellIsEightSeconds() {
        // Pinned constant: 8 s is the calm dwell that survived UX review.
        XCTAssertEqual(AdBannerQueue.dwellSeconds(for: .autoSkipped), 8.0,
            "Auto-skipped dwell must remain 8 s")
    }

    func testSuggestDwellIsTwelveSeconds() {
        // Pinned constant: 12 s gives the user a beat or two more to
        // decide whether to tap "Skip". If this changes, the suggest
        // banner UX needs to be re-reviewed — this assertion is a
        // canary for an implicit policy shift.
        XCTAssertEqual(AdBannerQueue.dwellSeconds(for: .suggest), 12.0,
            "Suggest dwell must remain 12 s")
    }
}
