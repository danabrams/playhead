import Foundation

enum AnchorType: String, Sendable, Codable {
    case disclosure
    case sponsorLexicon
    case url
    case promoCode
    case fmPositive
    case transitionMarker
}

enum AnchorPolarity: Sendable {
    case startAnchored
    case endAnchored
    case neutral
}

struct AnchorTypeConfig: Sendable, Equatable {
    let polarity: AnchorPolarity
    let windowDuration: TimeInterval
    let backwardSearchRadius: TimeInterval
    let forwardSearchRadius: TimeInterval
}

struct SpanHypothesisConfig: Sendable {
    let anchorTypeConfigByType: [AnchorType: AnchorTypeConfig]

    let maxIdleGapSeconds: TimeInterval
    let evidenceDecayRate: Double
    let minConfirmedEvidence: Double
    let minBodyWeight: Double

    init(
        disclosure: AnchorTypeConfig = Self.defaultDisclosureConfig,
        sponsorLexicon: AnchorTypeConfig = Self.defaultSponsorLexiconConfig,
        url: AnchorTypeConfig = Self.defaultURLConfig,
        promoCode: AnchorTypeConfig = Self.defaultPromoCodeConfig,
        fmPositive: AnchorTypeConfig = Self.defaultFMPositiveConfig,
        transitionMarker: AnchorTypeConfig = Self.defaultTransitionMarkerConfig,
        maxIdleGapSeconds: TimeInterval = 20,
        evidenceDecayRate: Double = 0.95,
        minConfirmedEvidence: Double = 2.5,
        minBodyWeight: Double = 1.5
    ) {
        self.anchorTypeConfigByType = [
            .disclosure: disclosure,
            .sponsorLexicon: sponsorLexicon,
            .url: url,
            .promoCode: promoCode,
            .fmPositive: fmPositive,
            .transitionMarker: transitionMarker,
        ]
        self.maxIdleGapSeconds = maxIdleGapSeconds
        self.evidenceDecayRate = evidenceDecayRate
        self.minConfirmedEvidence = minConfirmedEvidence
        self.minBodyWeight = minBodyWeight
    }

    static let defaultDisclosureConfig = AnchorTypeConfig(
        polarity: .startAnchored,
        windowDuration: 90,
        backwardSearchRadius: 15,
        forwardSearchRadius: 90
    )
    static let defaultSponsorLexiconConfig = AnchorTypeConfig(
        polarity: .startAnchored,
        windowDuration: 90,
        backwardSearchRadius: 15,
        forwardSearchRadius: 90
    )
    static let defaultURLConfig = AnchorTypeConfig(
        polarity: .endAnchored,
        windowDuration: 75,
        backwardSearchRadius: 75,
        forwardSearchRadius: 15
    )
    static let defaultPromoCodeConfig = AnchorTypeConfig(
        polarity: .endAnchored,
        windowDuration: 75,
        backwardSearchRadius: 75,
        forwardSearchRadius: 15
    )
    static let defaultFMPositiveConfig = AnchorTypeConfig(
        polarity: .neutral,
        windowDuration: 60,
        backwardSearchRadius: 30,
        forwardSearchRadius: 30
    )
    static let defaultTransitionMarkerConfig = AnchorTypeConfig(
        polarity: .endAnchored,
        windowDuration: 60,
        backwardSearchRadius: 60,
        forwardSearchRadius: 5
    )

    static let `default` = SpanHypothesisConfig()

    func config(for anchorType: AnchorType) -> AnchorTypeConfig {
        guard let config = anchorTypeConfigByType[anchorType] else {
            preconditionFailure("Missing anchor-type configuration for \(anchorType)")
        }
        return config
    }

    var maximumBackwardSearchRadius: TimeInterval {
        anchorTypeConfigByType.values.map(\.backwardSearchRadius).max() ?? 0
    }

    var maximumForwardSearchRadius: TimeInterval {
        anchorTypeConfigByType.values.map(\.forwardSearchRadius).max() ?? 0
    }

    var maximumContextPadding: TimeInterval {
        max(maximumBackwardSearchRadius, maximumForwardSearchRadius)
    }
}
