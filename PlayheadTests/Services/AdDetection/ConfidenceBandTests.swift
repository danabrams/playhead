// ConfidenceBandTests.swift
// ef2.6.3: Tests for ConfidenceBand, GrayBandAction, SpanTreatment, and
// ConfidenceBandClassifier — the gray-band markOnly UX backend.

import Foundation
import Testing
@testable import Playhead

// MARK: - ConfidenceBand

@Suite("ConfidenceBand")
struct ConfidenceBandTests {

    // MARK: - Threshold boundaries

    @Test("below candidate threshold returns .subCandidate")
    func belowCandidateIsSubCandidate() {
        let band = ConfidenceBand.classify(confidence: 0.39, thresholds: .default)
        #expect(band == .subCandidate)
    }

    @Test("exactly at candidate threshold returns .candidate")
    func exactlyAtCandidateIsCandidate() {
        let band = ConfidenceBand.classify(confidence: 0.40, thresholds: .default)
        #expect(band == .candidate)
    }

    @Test("between candidate and markOnly returns .candidate")
    func betweenCandidateAndMarkOnlyIsCandidate() {
        let band = ConfidenceBand.classify(confidence: 0.59, thresholds: .default)
        #expect(band == .candidate)
    }

    @Test("exactly at markOnly threshold returns .markOnly")
    func exactlyAtMarkOnlyIsMarkOnly() {
        let band = ConfidenceBand.classify(confidence: 0.60, thresholds: .default)
        #expect(band == .markOnly)
    }

    @Test("between markOnly and confirm returns .markOnly")
    func betweenMarkOnlyAndConfirmIsMarkOnly() {
        let band = ConfidenceBand.classify(confidence: 0.69, thresholds: .default)
        #expect(band == .markOnly)
    }

    @Test("exactly at confirm threshold returns .confirmed")
    func exactlyAtConfirmIsConfirmed() {
        let band = ConfidenceBand.classify(confidence: 0.70, thresholds: .default)
        #expect(band == .confirmed)
    }

    @Test("between confirm and autoSkip returns .confirmed")
    func betweenConfirmAndAutoSkipIsConfirmed() {
        let band = ConfidenceBand.classify(confidence: 0.79, thresholds: .default)
        #expect(band == .confirmed)
    }

    @Test("exactly at autoSkip threshold returns .autoSkip")
    func exactlyAtAutoSkipIsAutoSkip() {
        let band = ConfidenceBand.classify(confidence: 0.80, thresholds: .default)
        #expect(band == .autoSkip)
    }

    @Test("high confidence returns .autoSkip")
    func highConfidenceIsAutoSkip() {
        let band = ConfidenceBand.classify(confidence: 0.95, thresholds: .default)
        #expect(band == .autoSkip)
    }

    @Test("confidence 1.0 returns .autoSkip")
    func perfectConfidenceIsAutoSkip() {
        let band = ConfidenceBand.classify(confidence: 1.0, thresholds: .default)
        #expect(band == .autoSkip)
    }

    @Test("confidence 0.0 returns .subCandidate")
    func zeroConfidenceIsSubCandidate() {
        let band = ConfidenceBand.classify(confidence: 0.0, thresholds: .default)
        #expect(band == .subCandidate)
    }

    @Test("negative confidence returns .subCandidate")
    func negativeConfidenceIsSubCandidate() {
        let band = ConfidenceBand.classify(confidence: -0.1, thresholds: .default)
        #expect(band == .subCandidate)
    }

    @Test("NaN confidence returns .subCandidate")
    func nanConfidenceIsSubCandidate() {
        let band = ConfidenceBand.classify(confidence: Double.nan, thresholds: .default)
        #expect(band == .subCandidate)
    }

    @Test("infinity confidence returns .subCandidate")
    func infinityConfidenceIsSubCandidate() {
        let band = ConfidenceBand.classify(confidence: Double.infinity, thresholds: .default)
        #expect(band == .subCandidate)
    }

    // MARK: - Custom thresholds

    @Test("custom thresholds are respected")
    func customThresholds() {
        let custom = ConfidenceBandThresholds(
            candidate: 0.30, markOnly: 0.50, confirm: 0.65, autoSkip: 0.85
        )
        #expect(ConfidenceBand.classify(confidence: 0.29, thresholds: custom) == .subCandidate)
        #expect(ConfidenceBand.classify(confidence: 0.30, thresholds: custom) == .candidate)
        #expect(ConfidenceBand.classify(confidence: 0.50, thresholds: custom) == .markOnly)
        #expect(ConfidenceBand.classify(confidence: 0.65, thresholds: custom) == .confirmed)
        #expect(ConfidenceBand.classify(confidence: 0.85, thresholds: custom) == .autoSkip)
    }

    // MARK: - Default thresholds match product spec

    @Test("default thresholds match product-approved values")
    func defaultThresholdsMatchSpec() {
        let t = ConfidenceBandThresholds.default
        #expect(t.candidate == 0.40)
        #expect(t.markOnly == 0.60)
        #expect(t.confirm == 0.70)
        #expect(t.autoSkip == 0.80)
    }

    // MARK: - Band properties

    @Test("showsMarker is true only for markOnly and above")
    func showsMarkerProperty() {
        #expect(!ConfidenceBand.subCandidate.showsMarker)
        #expect(!ConfidenceBand.candidate.showsMarker)
        #expect(ConfidenceBand.markOnly.showsMarker)
        #expect(ConfidenceBand.confirmed.showsMarker)
        #expect(ConfidenceBand.autoSkip.showsMarker)
    }

    @Test("isAutoSkipEligible is true only for autoSkip")
    func isAutoSkipEligibleProperty() {
        #expect(!ConfidenceBand.subCandidate.isAutoSkipEligible)
        #expect(!ConfidenceBand.candidate.isAutoSkipEligible)
        #expect(!ConfidenceBand.markOnly.isAutoSkipEligible)
        #expect(!ConfidenceBand.confirmed.isAutoSkipEligible)
        #expect(ConfidenceBand.autoSkip.isAutoSkipEligible)
    }
}

// MARK: - GrayBandAction

@Suite("GrayBandAction")
struct GrayBandActionTests {

    @Test("all three actions exist and are distinct")
    func allActionsDistinct() {
        let actions: Set<GrayBandAction> = [
            .skipSegment,
            .alwaysSkipThirdPartyPaid,
            .dontSkipHousePromos
        ]
        #expect(actions.count == 3)
    }

    @Test("GrayBandAction is Codable round-trip")
    func codableRoundTrip() throws {
        for action in GrayBandAction.allCases {
            let data = try JSONEncoder().encode(action)
            let decoded = try JSONDecoder().decode(GrayBandAction.self, from: data)
            #expect(decoded == action)
        }
    }
}

// MARK: - SpanTreatment

@Suite("SpanTreatment")
struct SpanTreatmentTests {

    @Test("markOnly band carries gray-band actions")
    func markOnlyCarriesActions() {
        let treatment = SpanTreatment(
            band: .markOnly,
            confidence: 0.65,
            availableActions: GrayBandAction.allCases
        )
        #expect(treatment.band == .markOnly)
        #expect(treatment.availableActions.count == 3)
        #expect(!treatment.band.isAutoSkipEligible)
        #expect(treatment.band.showsMarker)
    }

    @Test("autoSkip band carries no gray-band actions")
    func autoSkipHasNoGrayBandActions() {
        let treatment = SpanTreatment(
            band: .autoSkip,
            confidence: 0.90,
            availableActions: []
        )
        #expect(treatment.band.isAutoSkipEligible)
        #expect(treatment.availableActions.isEmpty)
    }

    @Test("subCandidate band carries no actions and no marker")
    func subCandidateIsInert() {
        let treatment = SpanTreatment(
            band: .subCandidate,
            confidence: 0.20,
            availableActions: []
        )
        #expect(!treatment.band.showsMarker)
        #expect(!treatment.band.isAutoSkipEligible)
        #expect(treatment.availableActions.isEmpty)
    }

    @Test("markerLabel for markOnly is 'Likely sponsor segment'")
    func markOnlyMarkerLabel() {
        let treatment = SpanTreatment(
            band: .markOnly,
            confidence: 0.65,
            availableActions: GrayBandAction.allCases
        )
        #expect(treatment.markerLabel == "Likely sponsor segment")
    }

    @Test("markerLabel for confirmed is 'Sponsor segment'")
    func confirmedMarkerLabel() {
        let treatment = SpanTreatment(
            band: .confirmed,
            confidence: 0.75,
            availableActions: []
        )
        #expect(treatment.markerLabel == "Sponsor segment")
    }

    @Test("markerLabel is nil for bands that don't show markers")
    func noMarkerLabelForSubCandidateAndCandidate() {
        #expect(SpanTreatment(band: .subCandidate, confidence: 0.1, availableActions: []).markerLabel == nil)
        #expect(SpanTreatment(band: .candidate, confidence: 0.5, availableActions: []).markerLabel == nil)
    }
}

// MARK: - ConfidenceBandClassifier integration

@Suite("ConfidenceBandClassifier")
struct ConfidenceBandClassifierTests {

    let classifier = ConfidenceBandClassifier(thresholds: .default)

    @Test("classifies markOnly span with full action set")
    func markOnlySpanGetsTreatment() {
        let treatment = classifier.treatment(for: 0.65)
        #expect(treatment.band == .markOnly)
        #expect(treatment.confidence == 0.65)
        #expect(treatment.availableActions.contains(.skipSegment))
        #expect(treatment.availableActions.contains(.alwaysSkipThirdPartyPaid))
        #expect(treatment.availableActions.contains(.dontSkipHousePromos))
    }

    @Test("classifies autoSkip span with no gray-band actions")
    func autoSkipSpanGetsNoActions() {
        let treatment = classifier.treatment(for: 0.85)
        #expect(treatment.band == .autoSkip)
        #expect(treatment.availableActions.isEmpty)
    }

    @Test("classifies confirmed span with no gray-band actions")
    func confirmedSpanGetsNoActions() {
        let treatment = classifier.treatment(for: 0.75)
        #expect(treatment.band == .confirmed)
        #expect(treatment.availableActions.isEmpty)
    }

    @Test("classifies candidate span with no actions")
    func candidateSpanGetsNoActions() {
        let treatment = classifier.treatment(for: 0.50)
        #expect(treatment.band == .candidate)
        #expect(treatment.availableActions.isEmpty)
    }

    @Test("classifies subCandidate span with no actions")
    func subCandidateGetsNoActions() {
        let treatment = classifier.treatment(for: 0.20)
        #expect(treatment.band == .subCandidate)
        #expect(treatment.availableActions.isEmpty)
    }

    @Test("boundary: exactly 0.60 gets markOnly treatment")
    func boundaryMarkOnly() {
        let treatment = classifier.treatment(for: 0.60)
        #expect(treatment.band == .markOnly)
        #expect(!treatment.availableActions.isEmpty)
    }

    @Test("boundary: exactly 0.80 gets autoSkip, not markOnly")
    func boundaryAutoSkip() {
        let treatment = classifier.treatment(for: 0.80)
        #expect(treatment.band == .autoSkip)
        #expect(treatment.availableActions.isEmpty)
    }
}

// MARK: - AdDetectionConfig markOnly threshold integration

@Suite("AdDetectionConfig — markOnly threshold")
struct AdDetectionConfigMarkOnlyTests {

    @Test("default config has markOnlyThreshold of 0.60")
    func defaultMarkOnlyThreshold() {
        #expect(AdDetectionConfig.default.markOnlyThreshold == 0.60)
    }

    @Test("default autoSkipConfidenceThreshold updated to 0.80")
    func updatedAutoSkipThreshold() {
        #expect(AdDetectionConfig.default.autoSkipConfidenceThreshold == 0.80)
    }

    @Test("bandThresholds derives from config fields")
    func bandThresholdsDerivation() {
        let config = AdDetectionConfig.default
        let thresholds = config.bandThresholds
        #expect(thresholds.candidate == config.candidateThreshold)
        #expect(thresholds.markOnly == config.markOnlyThreshold)
        #expect(thresholds.confirm == config.confirmationThreshold)
        #expect(thresholds.autoSkip == config.autoSkipConfidenceThreshold)
    }

    @Test("custom markOnlyThreshold is stored correctly")
    func customMarkOnlyThreshold() {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-v1",
            markOnlyThreshold: 0.55,
            autoSkipConfidenceThreshold: 0.85
        )
        #expect(config.markOnlyThreshold == 0.55)
        #expect(config.autoSkipConfidenceThreshold == 0.85)
    }
}
