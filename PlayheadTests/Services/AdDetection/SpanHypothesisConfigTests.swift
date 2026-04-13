import Foundation
import Testing

@testable import Playhead

@Suite("SpanHypothesisConfig")
struct SpanHypothesisConfigTests {

    @Test("default config includes global defaults from phase A design table")
    func defaultGlobalValuesMatchDesign() {
        let config = SpanHypothesisConfig.default
        #expect(config.maxIdleGapSeconds == 20)
        #expect(config.evidenceDecayRate == 0.95)
        #expect(config.minConfirmedEvidence == 2.5)
        #expect(config.minBodyWeight == 1.5)
        #expect(config.anchorTypeConfigByType.count == 6)
    }

    @Test("default config(for:) returns the anchor-specific defaults")
    func defaultAnchorSpecificConfigLookup() {
        let config = SpanHypothesisConfig.default

        let disclosureConfig = config.config(for: .disclosure)
        #expect(disclosureConfig.polarity == .startAnchored)
        #expect(disclosureConfig.windowDuration == 90)
        #expect(disclosureConfig.backwardSearchRadius == 15)
        #expect(disclosureConfig.forwardSearchRadius == 90)

        let sponsorLexiconConfig = config.config(for: .sponsorLexicon)
        #expect(sponsorLexiconConfig.polarity == .startAnchored)
        #expect(sponsorLexiconConfig.windowDuration == 90)
        #expect(sponsorLexiconConfig.backwardSearchRadius == 15)
        #expect(sponsorLexiconConfig.forwardSearchRadius == 90)

        let urlConfig = config.config(for: .url)
        #expect(urlConfig.polarity == .endAnchored)
        #expect(urlConfig.windowDuration == 75)
        #expect(urlConfig.backwardSearchRadius == 75)
        #expect(urlConfig.forwardSearchRadius == 15)

        let promoConfig = config.config(for: .promoCode)
        #expect(promoConfig.polarity == .endAnchored)
        #expect(promoConfig.windowDuration == 75)
        #expect(promoConfig.backwardSearchRadius == 75)
        #expect(promoConfig.forwardSearchRadius == 15)

        let fmPositiveConfig = config.config(for: .fmPositive)
        #expect(fmPositiveConfig.polarity == .neutral)
        #expect(fmPositiveConfig.windowDuration == 60)
        #expect(fmPositiveConfig.backwardSearchRadius == 30)
        #expect(fmPositiveConfig.forwardSearchRadius == 30)

        let transitionConfig = config.config(for: .transitionMarker)
        #expect(transitionConfig.polarity == .endAnchored)
        #expect(transitionConfig.windowDuration == 60)
        #expect(transitionConfig.backwardSearchRadius == 60)
        #expect(transitionConfig.forwardSearchRadius == 5)
    }

    @Test("initializer allows focused overrides without rebuilding the full map")
    func initializerSupportsFocusedOverrides() {
        let customURL = AnchorTypeConfig(
            polarity: .neutral,
            windowDuration: 42,
            backwardSearchRadius: 12,
            forwardSearchRadius: 24
        )

        let config = SpanHypothesisConfig(
            url: customURL,
            maxIdleGapSeconds: 11
        )

        #expect(config.config(for: .url) == customURL)
        #expect(config.config(for: .disclosure) == SpanHypothesisConfig.defaultDisclosureConfig)
        #expect(config.maxIdleGapSeconds == 11)
        #expect(config.evidenceDecayRate == 0.95)
    }

    @Test("maximum context padding tracks the widest configured search radius")
    func maximumContextPaddingFollowsConfiguredSearchRadii() {
        let customURL = AnchorTypeConfig(
            polarity: .endAnchored,
            windowDuration: 75,
            backwardSearchRadius: 120,
            forwardSearchRadius: 12
        )
        let customDisclosure = AnchorTypeConfig(
            polarity: .startAnchored,
            windowDuration: 90,
            backwardSearchRadius: 10,
            forwardSearchRadius: 95
        )

        let config = SpanHypothesisConfig(
            disclosure: customDisclosure,
            url: customURL
        )

        #expect(config.maximumBackwardSearchRadius == 120)
        #expect(config.maximumForwardSearchRadius == 95)
        #expect(config.maximumContextPadding == 120)
    }
}
