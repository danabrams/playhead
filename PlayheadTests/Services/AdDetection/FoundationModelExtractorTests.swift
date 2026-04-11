// FoundationModelExtractorTests.swift
// Verification tests for playhead-8eh: Foundation Models Metadata Extraction (Layer 3).
//
// Covers: FallbackExtractor output format, AdMetadata schema shape,
// MetadataPromptVersion constant, MetadataExtractorFactory gating,
// AdBannerView confidence threshold, and banner copy degradation paths.
//
// Does NOT test FoundationModelExtractor directly (requires iOS 26 runtime).
// Instead verifies the contract surface that all callers depend on.

import Foundation
import Testing
@testable import Playhead

// MARK: - FallbackExtractor Tests

@Suite("FallbackExtractor output format")
struct FallbackExtractorOutputTests {

    @Test("source is 'fallback'")
    func sourceIsFallback() async throws {
        let extractor = FallbackExtractor()
        let result = try await extractor.extract(
            evidenceText: "This episode is brought to you by squarespace, where you can build your website.",
            windowStartTime: 120.0,
            windowEndTime: 180.0
        )
        #expect(result != nil)
        #expect(result?.source == "fallback")
    }

    @Test("confidence is low (0.1–0.3 range) with lexical signal")
    func confidenceRangeWithSignal() async throws {
        let extractor = FallbackExtractor()
        let result = try await extractor.extract(
            evidenceText: "This episode is brought to you by squarespace, where you can build your site.",
            windowStartTime: 100.0,
            windowEndTime: 160.0
        )
        #expect(result != nil)
        let confidence = try #require(result?.confidence)
        #expect(confidence >= 0.1 && confidence <= 0.3,
                "Fallback confidence with lexical signal should be in 0.1–0.3 range, got \(confidence)")
    }

    @Test("confidence is 0.1 when no lexical signal found")
    func confidenceWithoutSignal() async throws {
        let extractor = FallbackExtractor()
        let result = try await extractor.extract(
            evidenceText: "And then I was thinking about the weather last week.",
            windowStartTime: 200.0,
            windowEndTime: 230.0
        )
        #expect(result != nil)
        #expect(result?.confidence == 0.1,
                "Fallback confidence with no signal should be exactly 0.1")
    }

    @Test("extracts advertiser from 'brought to you by' pattern")
    func extractsAdvertiserFromBroughtToYouBy() async throws {
        let extractor = FallbackExtractor()
        let result = try await extractor.extract(
            evidenceText: "This episode is brought to you by squarespace, the all-in-one website platform.",
            windowStartTime: 0.0,
            windowEndTime: 30.0
        )
        #expect(result != nil)
        #expect(result?.advertiser != nil,
                "Should extract advertiser from 'brought to you by' pattern")
    }

    @Test("returns nil for empty evidence text")
    func returnsNilForEmptyEvidence() async throws {
        let extractor = FallbackExtractor()
        let result = try await extractor.extract(
            evidenceText: "",
            windowStartTime: 0.0,
            windowEndTime: 30.0
        )
        #expect(result == nil)
    }

    @Test("evidenceText is truncated to 200 characters")
    func evidenceTextTruncated() async throws {
        let extractor = FallbackExtractor()
        let longText = String(repeating: "word ", count: 100) // 500 chars
        let result = try await extractor.extract(
            evidenceText: longText,
            windowStartTime: 0.0,
            windowEndTime: 60.0
        )
        #expect(result != nil)
        #expect(result!.evidenceText.count <= 200,
                "Evidence text should be truncated to 200 characters max")
    }

    @Test("promptVersion matches MetadataPromptVersion.current")
    func promptVersionMatches() async throws {
        let extractor = FallbackExtractor()
        let result = try await extractor.extract(
            evidenceText: "Some ad content here for testing purposes.",
            windowStartTime: 0.0,
            windowEndTime: 30.0
        )
        #expect(result?.promptVersion == MetadataPromptVersion.current)
    }
}

// MARK: - AdMetadata Schema Tests

@Suite("AdMetadata schema shape")
struct AdMetadataSchemaTests {

    @Test("AdMetadata has expected fields and no adDescription")
    func schemaFields() {
        let metadata = AdMetadata(
            advertiser: "TestBrand",
            product: "TestProduct",
            evidenceText: "brought to you by TestBrand",
            confidence: 0.85,
            promptVersion: "metadata-v1",
            source: "foundationModels"
        )

        #expect(metadata.advertiser == "TestBrand")
        #expect(metadata.product == "TestProduct")
        #expect(metadata.evidenceText == "brought to you by TestBrand")
        #expect(metadata.confidence == 0.85)
        #expect(metadata.promptVersion == "metadata-v1")
        #expect(metadata.source == "foundationModels")
    }

    @Test("AdMetadata supports nil advertiser and product")
    func optionalFields() {
        let metadata = AdMetadata(
            advertiser: nil,
            product: nil,
            evidenceText: "some text",
            confidence: 0.1,
            promptVersion: "metadata-v1",
            source: "fallback"
        )

        #expect(metadata.advertiser == nil)
        #expect(metadata.product == nil)
    }

    @Test("AdMetadata is Codable round-trip safe")
    func codableRoundTrip() throws {
        let original = AdMetadata(
            advertiser: "Squarespace",
            product: "Website builder",
            evidenceText: "brought to you by squarespace",
            confidence: 0.92,
            promptVersion: "metadata-v1",
            source: "foundationModels"
        )

        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(AdMetadata.self, from: data)

        #expect(decoded == original)
    }

    @Test("AdMetadata source values match spec constants")
    func sourceValues() {
        let fmMetadata = AdMetadata(
            advertiser: nil,
            product: nil,
            evidenceText: "test",
            confidence: 0.5,
            promptVersion: "metadata-v1",
            source: "foundationModels"
        )
        #expect(fmMetadata.source == "foundationModels")

        let fallbackMetadata = AdMetadata(
            advertiser: nil,
            product: nil,
            evidenceText: "test",
            confidence: 0.1,
            promptVersion: "metadata-v1",
            source: "fallback"
        )
        #expect(fallbackMetadata.source == "fallback")
    }
}

// MARK: - MetadataPromptVersion Tests

@Suite("MetadataPromptVersion")
struct MetadataPromptVersionTests {

    @Test("current version is 'metadata-v1'")
    func currentVersion() {
        #expect(MetadataPromptVersion.current == "metadata-v1")
    }
}

// MARK: - MetadataExtractorFactory Tests

@Suite("MetadataExtractorFactory gating")
struct MetadataExtractorFactoryTests {

    private func makeSnapshot(canUseFM: Bool) -> CapabilitySnapshot {
        CapabilitySnapshot(
            foundationModelsAvailable: canUseFM,
            foundationModelsUsable: canUseFM,
            appleIntelligenceEnabled: canUseFM,
            foundationModelsLocaleSupported: canUseFM,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000_000,
            capturedAt: Date()
        )
    }

    @Test("returns FallbackExtractor when FM unavailable")
    func fallbackWhenUnavailable() {
        let snapshot = makeSnapshot(canUseFM: false)
        let extractor = MetadataExtractorFactory.makeExtractor(snapshot: snapshot)
        #expect(extractor is FallbackExtractor,
                "Factory should return FallbackExtractor when canUseFoundationModels is false")
    }

    @Test("locale not supported produces fallback extractor")
    func localeNotSupportedProducesFallback() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000_000,
            capturedAt: Date()
        )
        #expect(snapshot.canUseFoundationModels == false,
                "canUseFoundationModels should be false when locale is unsupported")
        let extractor = MetadataExtractorFactory.makeExtractor(snapshot: snapshot)
        #expect(extractor is FallbackExtractor)
    }

    @Test("needsReExtraction returns true when no prompt version")
    func needsReExtractionNilVersion() {
        #expect(MetadataExtractorFactory.needsReExtraction(
            currentPromptVersion: nil,
            currentSource: nil
        ) == true)
    }

    @Test("needsReExtraction returns false for current version")
    func noReExtractionForCurrentVersion() {
        #expect(MetadataExtractorFactory.needsReExtraction(
            currentPromptVersion: MetadataPromptVersion.current,
            currentSource: "foundationModels"
        ) == false)
    }

    @Test("needsReExtraction returns true for stale version")
    func reExtractionForStaleVersion() {
        #expect(MetadataExtractorFactory.needsReExtraction(
            currentPromptVersion: "metadata-v0-stale",
            currentSource: "foundationModels"
        ) == true)
    }
}

// MARK: - CapabilitySnapshot FM Gating Tests

@Suite("CapabilitySnapshot canUseFoundationModels gating")
struct CapabilitySnapshotFMGatingTests {

    @Test("canUseFoundationModels requires all four flags true")
    func requiresAllFlags() {
        let full = CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000_000,
            capturedAt: Date()
        )
        #expect(full.canUseFoundationModels == true)
    }

    @Test("canUseFoundationModels false when AI disabled")
    func falseWhenAIDisabled() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000_000,
            capturedAt: Date()
        )
        #expect(snapshot.canUseFoundationModels == false)
    }

    @Test("canUseFoundationModels false when FM not usable (probe failed)")
    func falseWhenProbeNotUsable() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: false,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000_000,
            capturedAt: Date()
        )
        #expect(snapshot.canUseFoundationModels == false)
    }
}

// MARK: - AdBannerView Confidence Gate Tests

@Suite("AdBannerView confidence gate (playhead-8eh)")
@MainActor
struct AdBannerConfidenceGateTests {

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

    @Test("metadataConfidenceThreshold is 0.60")
    func thresholdValue() {
        #expect(AdBannerView.metadataConfidenceThreshold == 0.60)
    }

    @Test("high confidence FM result shows brand name")
    func highConfidenceShowsBrand() {
        let item = makeBannerItem(
            advertiser: "Squarespace",
            product: "Build your website",
            metadataConfidence: 0.85,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)
        #expect(copy.prefix == "Skipped")
        #expect(copy.advertiser == "Squarespace")
        #expect(copy.detail == "Build your website")
    }

    @Test("low confidence shows generic copy, never surfaces brand")
    func lowConfidenceShowsGeneric() {
        let item = makeBannerItem(
            advertiser: "Maybe Corp",
            product: "Something",
            metadataConfidence: 0.25,
            metadataSource: "fallback"
        )
        let copy = AdBannerView.bannerCopy(for: item)
        #expect(copy.prefix == "Skipped sponsor segment")
        #expect(copy.advertiser == nil)
        #expect(copy.detail == nil)
    }

    @Test("nil metadata produces generic banner copy")
    func nilMetadataGenericCopy() {
        let item = makeBannerItem(
            advertiser: nil,
            product: nil,
            metadataConfidence: nil,
            metadataSource: "none"
        )
        let copy = AdBannerView.bannerCopy(for: item)
        #expect(copy.prefix == "Skipped sponsor segment")
        #expect(copy.advertiser == nil)
        #expect(copy.detail == nil)
    }

    @Test("fallback source at confidence 0.3 shows generic (below threshold)")
    func fallbackSourceBelowThreshold() {
        let item = makeBannerItem(
            advertiser: "Squarespace",
            product: nil,
            metadataConfidence: 0.3,
            metadataSource: "fallback"
        )
        let copy = AdBannerView.bannerCopy(for: item)
        #expect(copy.prefix == "Skipped sponsor segment",
                "Fallback confidence 0.3 is below 0.60 threshold — must show generic")
        #expect(copy.advertiser == nil)
    }

    @Test("exact threshold confidence surfaces advertiser")
    func exactThresholdSurfacesAdvertiser() {
        let item = makeBannerItem(
            advertiser: "Athletic Greens",
            product: nil,
            metadataConfidence: 0.60,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)
        #expect(copy.prefix == "Skipped")
        #expect(copy.advertiser == "Athletic Greens")
    }

    @Test("just below threshold does not surface advertiser")
    func justBelowThreshold() {
        let item = makeBannerItem(
            advertiser: "Athletic Greens",
            product: nil,
            metadataConfidence: 0.59,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)
        #expect(copy.prefix == "Skipped sponsor segment")
        #expect(copy.advertiser == nil)
    }

    @Test("metadataSource 'none' suppresses even high confidence")
    func sourceNoneSuppresses() {
        let item = makeBannerItem(
            advertiser: "Real Brand",
            product: "Real Product",
            metadataConfidence: 0.99,
            metadataSource: "none"
        )
        let copy = AdBannerView.bannerCopy(for: item)
        #expect(copy.prefix == "Skipped sponsor segment")
        #expect(copy.advertiser == nil)
        #expect(copy.detail == nil)
    }

    @Test("banner copy is template-driven, not free-form")
    func templateDriven() {
        let item = makeBannerItem(
            advertiser: "HelloFresh",
            product: "Meal kits",
            metadataConfidence: 0.80,
            metadataSource: "foundationModels"
        )
        let copy = AdBannerView.bannerCopy(for: item)
        // Prefix is always a fixed literal
        #expect(copy.prefix == "Skipped")
        // Advertiser/detail are passthrough, never rewritten
        #expect(copy.advertiser == "HelloFresh")
        #expect(copy.detail == "Meal kits")
    }
}
