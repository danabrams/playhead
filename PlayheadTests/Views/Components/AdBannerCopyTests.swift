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
        metadataSource: String = "none"
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
            evidenceCatalogEntries: []
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
}
