// SpanDecisionTests.swift
// ef2.4.1: Tests for the four-stage pipeline output type (SpanDecision).
//
// Covers: ProposalAuthority quorum logic, ContentClass enum, SkipEligibility,
// SkipPolicyMatrixV2, BoundaryEstimate, and SpanDecision composition.

import Foundation
import Testing
@testable import Playhead

// MARK: - ProposalAuthority Tests

@Suite("ProposalAuthority")
struct ProposalAuthorityTests {

    // MARK: - EvidenceFamily classification

    @Test("url is strong, lexical family")
    func urlIsStrongLexical() {
        let sig = ProposalSignal.url
        #expect(sig.authority == .strong)
        #expect(sig.family == .lexical)
    }

    @Test("promoCode is strong, lexical family")
    func promoCodeIsStrongLexical() {
        let sig = ProposalSignal.promoCode
        #expect(sig.authority == .strong)
        #expect(sig.family == .lexical)
    }

    @Test("disclosure is strong, lexical family")
    func disclosureIsStrongLexical() {
        let sig = ProposalSignal.disclosure
        #expect(sig.authority == .strong)
        #expect(sig.family == .lexical)
    }

    @Test("fmContainsAd is strong, model family")
    func fmContainsAdIsStrongModel() {
        let sig = ProposalSignal.fmContainsAd
        #expect(sig.authority == .strong)
        #expect(sig.family == .model)
    }

    @Test("fingerprint is strong, acoustic family")
    func fingerprintIsStrongAcoustic() {
        let sig = ProposalSignal.fingerprint
        #expect(sig.authority == .strong)
        #expect(sig.family == .acoustic)
    }

    @Test("metadata is weak, metadata family")
    func metadataIsWeakMetadata() {
        let sig = ProposalSignal.metadata
        #expect(sig.authority == .weak)
        #expect(sig.family == .metadata)
    }

    @Test("positionPrior is weak, heuristic family")
    func positionPriorIsWeakHeuristic() {
        let sig = ProposalSignal.positionPrior
        #expect(sig.authority == .weak)
        #expect(sig.family == .heuristic)
    }

    @Test("musicBracket is weak, acoustic family")
    func musicBracketIsWeakAcoustic() {
        let sig = ProposalSignal.musicBracket
        #expect(sig.authority == .weak)
        #expect(sig.family == .acoustic)
    }

    @Test("lexicalWithoutAnchor is weak, lexical family")
    func lexicalWithoutAnchorIsWeakLexical() {
        let sig = ProposalSignal.lexicalWithoutAnchor
        #expect(sig.authority == .weak)
        #expect(sig.family == .lexical)
    }

    // MARK: - Quorum rules

    @Test("single strong signal meets quorum")
    func singleStrongMeetsQuorum() {
        let signals: [ProposalSignal] = [.fmContainsAd]
        #expect(ProposalQuorum.isMet(signals: signals))
    }

    @Test("single weak signal does not meet quorum")
    func singleWeakDoesNotMeetQuorum() {
        let signals: [ProposalSignal] = [.metadata]
        #expect(!ProposalQuorum.isMet(signals: signals))
    }

    @Test("two weak signals from same family do not meet quorum")
    func twoWeakSameFamilyDoNotMeetQuorum() {
        // Both are lexical family
        let signals: [ProposalSignal] = [.lexicalWithoutAnchor, .lexicalWithoutAnchor]
        #expect(!ProposalQuorum.isMet(signals: signals))
    }

    @Test("two weak signals from different families meet quorum")
    func twoWeakDifferentFamiliesMeetQuorum() {
        let signals: [ProposalSignal] = [.metadata, .musicBracket]
        #expect(ProposalQuorum.isMet(signals: signals))
    }

    @Test("empty signals do not meet quorum")
    func emptySignalsDoNotMeetQuorum() {
        #expect(!ProposalQuorum.isMet(signals: []))
    }

    @Test("three weak signals from same family still do not meet quorum")
    func threeWeakSameFamilyStillFails() {
        let signals: [ProposalSignal] = [.metadata, .metadata, .metadata]
        #expect(!ProposalQuorum.isMet(signals: signals))
    }

    @Test("quorum resolvedAuthority is strong when any strong signal present")
    func resolvedAuthorityStrongWhenStrongPresent() {
        let signals: [ProposalSignal] = [.fmContainsAd, .metadata]
        #expect(ProposalQuorum.resolvedAuthority(signals: signals) == .strong)
    }

    @Test("quorum resolvedAuthority is weak when only weak signals from different families")
    func resolvedAuthorityWeakWhenOnlyWeak() {
        let signals: [ProposalSignal] = [.metadata, .musicBracket]
        #expect(ProposalQuorum.resolvedAuthority(signals: signals) == .weak)
    }

    @Test("quorum resolvedAuthority is nil when quorum not met")
    func resolvedAuthorityNilWhenNotMet() {
        let signals: [ProposalSignal] = [.metadata]
        #expect(ProposalQuorum.resolvedAuthority(signals: signals) == nil)
    }
}

// MARK: - ContentClass Tests

@Suite("ContentClass")
struct ContentClassTests {

    @Test("ContentClass has all seven cases")
    func allCases() {
        let cases = ContentClass.allCases
        #expect(cases.count == 7)
        #expect(cases.contains(.thirdPartyPaid))
        #expect(cases.contains(.affiliatePaid))
        #expect(cases.contains(.networkPromo))
        #expect(cases.contains(.showPromo))
        #expect(cases.contains(.ownedProduct))
        #expect(cases.contains(.editorialMention))
        #expect(cases.contains(.unknown))
    }

    @Test("ContentClass rawValues are stable for persistence")
    func rawValuesAreStable() {
        #expect(ContentClass.thirdPartyPaid.rawValue == "thirdPartyPaid")
        #expect(ContentClass.affiliatePaid.rawValue == "affiliatePaid")
        #expect(ContentClass.networkPromo.rawValue == "networkPromo")
        #expect(ContentClass.showPromo.rawValue == "showPromo")
        #expect(ContentClass.ownedProduct.rawValue == "ownedProduct")
        #expect(ContentClass.editorialMention.rawValue == "editorialMention")
        #expect(ContentClass.unknown.rawValue == "unknown")
    }
}

// MARK: - BoundaryEstimate Tests

@Suite("BoundaryEstimate")
struct BoundaryEstimateTests {

    @Test("BoundaryEstimate stores start and end times with confidence")
    func storesTimesAndConfidence() {
        let estimate = BoundaryEstimate(
            startTime: 10.0,
            endTime: 40.0,
            startConfidence: 0.9,
            endConfidence: 0.7
        )
        #expect(estimate.startTime == 10.0)
        #expect(estimate.endTime == 40.0)
        #expect(estimate.startConfidence == 0.9)
        #expect(estimate.endConfidence == 0.7)
    }

    @Test("BoundaryEstimate duration is computed correctly")
    func durationIsCorrect() {
        let estimate = BoundaryEstimate(
            startTime: 15.0,
            endTime: 45.0,
            startConfidence: 0.8,
            endConfidence: 0.8
        )
        #expect(estimate.duration == 30.0)
    }
}

// MARK: - SkipEligibility Tests

@Suite("SkipEligibility")
struct SkipEligibilityTests {

    @Test("SkipEligibility has all four cases")
    func allCases() {
        let cases = SkipEligibility.allCases
        #expect(cases.count == 4)
        #expect(cases.contains(.autoSkipEligible))
        #expect(cases.contains(.markOnly))
        #expect(cases.contains(.userConfigurable))
        #expect(cases.contains(.ineligible))
    }

    @Test("SkipEligibility rawValues are stable for persistence")
    func rawValuesAreStable() {
        #expect(SkipEligibility.autoSkipEligible.rawValue == "autoSkipEligible")
        #expect(SkipEligibility.markOnly.rawValue == "markOnly")
        #expect(SkipEligibility.userConfigurable.rawValue == "userConfigurable")
        #expect(SkipEligibility.ineligible.rawValue == "ineligible")
    }
}

// MARK: - SkipPolicyMatrixV2 Tests

@Suite("SkipPolicyMatrixV2")
struct SkipPolicyMatrixV2Tests {

    @Test("thirdPartyPaid + strong → autoSkipEligible")
    func thirdPartyPaidStrongIsAutoSkip() {
        #expect(SkipPolicyMatrixV2.eligibility(for: .thirdPartyPaid, authority: .strong) == .autoSkipEligible)
    }

    @Test("thirdPartyPaid + weak → markOnly (weak evidence demotes to banner)")
    func thirdPartyPaidWeakIsMarkOnly() {
        #expect(SkipPolicyMatrixV2.eligibility(for: .thirdPartyPaid, authority: .weak) == .markOnly)
    }

    @Test("affiliatePaid + strong → markOnly (affiliate reads always banner-only)")
    func affiliatePaidStrongIsMarkOnly() {
        #expect(SkipPolicyMatrixV2.eligibility(for: .affiliatePaid, authority: .strong) == .markOnly)
    }

    @Test("affiliatePaid + weak → markOnly")
    func affiliatePaidWeakIsMarkOnly() {
        #expect(SkipPolicyMatrixV2.eligibility(for: .affiliatePaid, authority: .weak) == .markOnly)
    }

    @Test("networkPromo + strong → userConfigurable")
    func networkPromoStrongIsUserConfigurable() {
        #expect(SkipPolicyMatrixV2.eligibility(for: .networkPromo, authority: .strong) == .userConfigurable)
    }

    @Test("networkPromo + weak → markOnly")
    func networkPromoWeakIsMarkOnly() {
        #expect(SkipPolicyMatrixV2.eligibility(for: .networkPromo, authority: .weak) == .markOnly)
    }

    @Test("showPromo + strong → userConfigurable")
    func showPromoStrongIsUserConfigurable() {
        #expect(SkipPolicyMatrixV2.eligibility(for: .showPromo, authority: .strong) == .userConfigurable)
    }

    @Test("showPromo + weak → markOnly")
    func showPromoWeakIsMarkOnly() {
        #expect(SkipPolicyMatrixV2.eligibility(for: .showPromo, authority: .weak) == .markOnly)
    }

    @Test("ownedProduct + any authority → markOnly", arguments: [ProposalAuthority.strong, ProposalAuthority.weak])
    func ownedProductIsAlwaysMarkOnly(authority: ProposalAuthority) {
        #expect(SkipPolicyMatrixV2.eligibility(for: .ownedProduct, authority: authority) == .markOnly)
    }

    @Test("editorialMention + any authority → ineligible", arguments: [ProposalAuthority.strong, ProposalAuthority.weak])
    func editorialMentionIsAlwaysIneligible(authority: ProposalAuthority) {
        #expect(SkipPolicyMatrixV2.eligibility(for: .editorialMention, authority: authority) == .ineligible)
    }

    @Test("unknown + strong → markOnly (uncertain class, strong evidence → surface banner)")
    func unknownStrongIsMarkOnly() {
        #expect(SkipPolicyMatrixV2.eligibility(for: .unknown, authority: .strong) == .markOnly)
    }

    @Test("unknown + weak → ineligible (insufficient signal everywhere)")
    func unknownWeakIsIneligible() {
        #expect(SkipPolicyMatrixV2.eligibility(for: .unknown, authority: .weak) == .ineligible)
    }

    @Test("policy is a pure lookup — same inputs always produce same output")
    func pureFunction() {
        let a = SkipPolicyMatrixV2.eligibility(for: .thirdPartyPaid, authority: .strong)
        let b = SkipPolicyMatrixV2.eligibility(for: .thirdPartyPaid, authority: .strong)
        #expect(a == b)
    }
}

// MARK: - SpanDecision Tests

@Suite("SpanDecision")
struct SpanDecisionTests {

    @Test("SpanDecision composes all four stages")
    func composesAllFourStages() {
        let decision = SpanDecision(
            proposalAuthority: .strong,
            proposalSignals: [.fmContainsAd, .url],
            contentClass: .thirdPartyPaid,
            boundary: BoundaryEstimate(
                startTime: 10.0,
                endTime: 40.0,
                startConfidence: 0.9,
                endConfidence: 0.8
            ),
            skipEligibility: .autoSkipEligible
        )
        #expect(decision.proposalAuthority == .strong)
        #expect(decision.proposalSignals.count == 2)
        #expect(decision.contentClass == .thirdPartyPaid)
        #expect(decision.boundary.startTime == 10.0)
        #expect(decision.boundary.endTime == 40.0)
        #expect(decision.skipEligibility == .autoSkipEligible)
    }

    @Test("SpanDecision is Sendable and Equatable")
    func isSendableAndEquatable() {
        let a = SpanDecision(
            proposalAuthority: .strong,
            proposalSignals: [.fmContainsAd],
            contentClass: .thirdPartyPaid,
            boundary: BoundaryEstimate(startTime: 10.0, endTime: 40.0, startConfidence: 0.9, endConfidence: 0.8),
            skipEligibility: .autoSkipEligible
        )
        let b = SpanDecision(
            proposalAuthority: .strong,
            proposalSignals: [.fmContainsAd],
            contentClass: .thirdPartyPaid,
            boundary: BoundaryEstimate(startTime: 10.0, endTime: 40.0, startConfidence: 0.9, endConfidence: 0.8),
            skipEligibility: .autoSkipEligible
        )
        #expect(a == b)
    }

    @Test("SpanDecision with different eligibility is not equal")
    func differentEligibilityNotEqual() {
        let a = SpanDecision(
            proposalAuthority: .strong,
            proposalSignals: [.fmContainsAd],
            contentClass: .thirdPartyPaid,
            boundary: BoundaryEstimate(startTime: 10.0, endTime: 40.0, startConfidence: 0.9, endConfidence: 0.8),
            skipEligibility: .autoSkipEligible
        )
        let b = SpanDecision(
            proposalAuthority: .strong,
            proposalSignals: [.fmContainsAd],
            contentClass: .thirdPartyPaid,
            boundary: BoundaryEstimate(startTime: 10.0, endTime: 40.0, startConfidence: 0.9, endConfidence: 0.8),
            skipEligibility: .markOnly
        )
        #expect(a != b)
    }
}
