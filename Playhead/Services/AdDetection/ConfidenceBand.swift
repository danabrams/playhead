// ConfidenceBand.swift
// ef2.6.3: Gray-band markOnly UX — confidence band classification, treatment
// model, and one-tap actions for sub-autoSkip spans.
//
// Design:
//   ConfidenceBand maps a continuous skipConfidence score to a discrete treatment
//   band. Five bands: subCandidate (<0.40), candidate (0.40–0.60), markOnly
//   (0.60–0.70), confirmed (0.70–0.80), autoSkip (≥0.80). Only autoSkip auto-skips;
//   the markOnly band surfaces a lightweight "likely sponsor segment" marker with
//   one-tap actions.
//
//   ConfidenceBandThresholds stores the four boundary values; defaults match the
//   product-approved spec. AdDetectionConfig derives a ConfidenceBandThresholds
//   from its existing threshold fields plus the new markOnlyThreshold.

import Foundation

// MARK: - ConfidenceBandThresholds

/// Product-approved threshold boundaries for confidence band classification.
struct ConfidenceBandThresholds: Sendable, Equatable {
    /// Minimum confidence to consider a span at all.
    let candidate: Double   // 0.40
    /// Minimum confidence to show a lightweight marker (no auto-skip).
    let markOnly: Double    // 0.60
    /// Minimum confidence to treat as a confirmed ad.
    let confirm: Double     // 0.70
    /// Minimum confidence to auto-skip.
    let autoSkip: Double    // 0.80

    init(candidate: Double, markOnly: Double, confirm: Double, autoSkip: Double) {
        assert(candidate < markOnly && markOnly < confirm && confirm < autoSkip,
            "ConfidenceBandThresholds should be monotonically increasing: candidate(\(candidate)) < markOnly(\(markOnly)) < confirm(\(confirm)) < autoSkip(\(autoSkip))")
        self.candidate = candidate
        self.markOnly = markOnly
        self.confirm = confirm
        self.autoSkip = autoSkip
    }

    static let `default` = ConfidenceBandThresholds(
        candidate: 0.40,
        markOnly: 0.60,
        confirm: 0.70,
        autoSkip: 0.80
    )
}

// MARK: - ConfidenceBand

/// Discrete treatment band derived from a continuous confidence score.
enum ConfidenceBand: String, Sendable, Codable, Hashable, CaseIterable {
    /// Below candidate threshold — not actionable.
    case subCandidate
    /// Meets candidate threshold but not markOnly — internal tracking only.
    case candidate
    /// Meets markOnly threshold — show lightweight marker, offer one-tap actions.
    case markOnly
    /// Meets confirmation threshold — confirmed ad, show banner.
    case confirmed
    /// Meets auto-skip threshold — eligible for automatic skipping.
    case autoSkip

    /// Classify a confidence score into a band using the given thresholds.
    /// NaN falls through to `.subCandidate` (IEEE 754: NaN >= x is always false).
    static func classify(confidence: Double, thresholds: ConfidenceBandThresholds) -> ConfidenceBand {
        guard confidence.isFinite else { return .subCandidate }
        if confidence >= thresholds.autoSkip { return .autoSkip }
        if confidence >= thresholds.confirm { return .confirmed }
        if confidence >= thresholds.markOnly { return .markOnly }
        if confidence >= thresholds.candidate { return .candidate }
        return .subCandidate
    }

    /// Whether this band warrants showing a visible marker in the UI.
    var showsMarker: Bool {
        switch self {
        case .markOnly, .confirmed, .autoSkip: return true
        case .subCandidate, .candidate: return false
        }
    }

    /// Whether this band is eligible for automatic skipping.
    var isAutoSkipEligible: Bool {
        self == .autoSkip
    }
}

// MARK: - GrayBandAction

/// One-tap user actions available for spans in the markOnly confidence band.
/// These actions let the user disambiguate gray-band spans without requiring
/// the system to make a high-confidence decision.
enum GrayBandAction: String, Sendable, Codable, Hashable, CaseIterable {
    /// Skip this specific segment right now.
    case skipSegment
    /// Always skip third-party paid ads (learn preference).
    case alwaysSkipThirdPartyPaid
    /// Don't skip house promos (learn preference).
    case dontSkipHousePromos
}

// MARK: - SpanTreatment

/// The treatment decision for a single span: which confidence band it falls in,
/// the raw confidence, and what user actions are available.
/// This is the data model the UI layer consumes to render markers and actions.
struct SpanTreatment: Sendable, Equatable {
    let band: ConfidenceBand
    let confidence: Double
    let availableActions: [GrayBandAction]

    /// User-facing label for the marker, if one should be shown.
    var markerLabel: String? {
        switch band {
        case .markOnly:
            return "Likely sponsor segment"
        case .confirmed, .autoSkip:
            return "Sponsor segment"
        case .subCandidate, .candidate:
            return nil
        }
    }
}

// MARK: - ConfidenceBandClassifier

/// Classifies a confidence score into a SpanTreatment with appropriate actions.
/// Stateless — all behavior is determined by the thresholds.
struct ConfidenceBandClassifier: Sendable {
    let thresholds: ConfidenceBandThresholds

    /// Produce a treatment for the given confidence score.
    func treatment(for confidence: Double) -> SpanTreatment {
        let band = ConfidenceBand.classify(confidence: confidence, thresholds: thresholds)
        let actions: [GrayBandAction]
        switch band {
        case .markOnly:
            actions = GrayBandAction.allCases
        case .subCandidate, .candidate, .confirmed, .autoSkip:
            actions = []
        }
        return SpanTreatment(band: band, confidence: confidence, availableActions: actions)
    }
}
