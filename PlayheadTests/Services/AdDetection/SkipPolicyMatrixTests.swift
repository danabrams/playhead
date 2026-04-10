// SkipPolicyMatrixTests.swift
// Phase 6 (playhead-4my.6.2): Tests for SkipPolicyMatrix, DecisionCohort, DecisionStabilityPolicy.

import Foundation
import Testing
@testable import Playhead

@Suite("SkipPolicyMatrix")
struct SkipPolicyMatrixTests {

    // MARK: - Policy matrix mappings

    @Test("paid + thirdParty → autoSkipEligible")
    func paidThirdPartyMapsToAutoSkip() {
        #expect(SkipPolicyMatrix.action(for: .paid, ownership: .thirdParty) == .autoSkipEligible)
    }

    @Test("owned + show → detectOnly")
    func ownedShowMapsToDetectOnly() {
        #expect(SkipPolicyMatrix.action(for: .owned, ownership: .show) == .detectOnly)
    }

    @Test("owned + network → detectOnly")
    func ownedNetworkMapsToDetectOnly() {
        #expect(SkipPolicyMatrix.action(for: .owned, ownership: .network) == .detectOnly)
    }

    @Test("affiliate + any ownership → detectOnly", arguments: AdOwnership.allCases)
    func affiliateAlwaysDetectOnly(ownership: AdOwnership) {
        #expect(SkipPolicyMatrix.action(for: .affiliate, ownership: ownership) == .detectOnly)
    }

    @Test("organic + any ownership → suppress", arguments: AdOwnership.allCases)
    func organicAlwaysSuppress(ownership: AdOwnership) {
        #expect(SkipPolicyMatrix.action(for: .organic, ownership: ownership) == .suppress)
    }

    @Test("unknown intent → logOnly for all ownerships", arguments: AdOwnership.allCases)
    func unknownIntentAlwaysLogOnly(ownership: AdOwnership) {
        #expect(SkipPolicyMatrix.action(for: .unknown, ownership: ownership) == .logOnly)
    }

    @Test("paid/owned + unknown ownership → logOnly (ownership needed for decision)")
    func paidOrOwnedWithUnknownOwnershipLogsOnly() {
        // paid needs .thirdParty confirmation; owned needs .show/.network confirmation.
        // Unknown ownership means we can't make a determination.
        #expect(SkipPolicyMatrix.action(for: .paid, ownership: .unknown) == .logOnly)
        #expect(SkipPolicyMatrix.action(for: .owned, ownership: .unknown) == .logOnly)
    }

    @Test("v1 default: unknown + unknown → logOnly (the actual v1 path)")
    func v1DefaultIsLogOnly() {
        #expect(SkipPolicyMatrix.action(for: .unknown, ownership: .unknown) == .logOnly)
        #expect(SkipPolicyMatrix.defaultAction == .logOnly)
    }
}

@Suite("DecisionCohort")
struct DecisionCohortTests {

    @Test("DecisionCohort is Hashable — equal cohorts hash equal")
    func equalCohortsHashEqual() {
        let a = DecisionCohort(
            featurePipelineHash: "fp1", fusionHash: "fu1",
            policyHash: "p1", stabilityHash: "s1", appBuild: "42"
        )
        let b = DecisionCohort(
            featurePipelineHash: "fp1", fusionHash: "fu1",
            policyHash: "p1", stabilityHash: "s1", appBuild: "42"
        )
        #expect(a == b)
        #expect(a.hashValue == b.hashValue)
    }

    @Test("DecisionCohort is Hashable — different cohorts differ")
    func differentCohortsAreNotEqual() {
        let a = DecisionCohort(
            featurePipelineHash: "fp1", fusionHash: "fu1",
            policyHash: "p1", stabilityHash: "s1", appBuild: "42"
        )
        let b = DecisionCohort(
            featurePipelineHash: "fp2", fusionHash: "fu1",
            policyHash: "p1", stabilityHash: "s1", appBuild: "42"
        )
        #expect(a != b)
    }

    @Test("DecisionCohort Codable round-trip")
    func codableRoundTrip() throws {
        let cohort = DecisionCohort.production(appBuild: "100")
        let data = try JSONEncoder().encode(cohort)
        let decoded = try JSONDecoder().decode(DecisionCohort.self, from: data)
        #expect(cohort == decoded)
    }

    @Test("DecisionCohort usable as dictionary key")
    func usableAsDictionaryKey() {
        let cohort = DecisionCohort.production(appBuild: "1")
        var cache: [DecisionCohort: String] = [:]
        cache[cohort] = "result"
        #expect(cache[cohort] == "result")
    }
}

@Suite("DecisionStabilityPolicy")
struct DecisionStabilityPolicyTests {

    let policy = DecisionStabilityPolicy.default // stayThreshold=0.45, suppressionThreshold=0.25

    // MARK: - User correction override

    @Test("user correction always allows removal regardless of score")
    func userCorrectionAlwaysAllowsRemoval() {
        // Even with high score, user correction wins
        #expect(policy.canRemoveCue(
            currentScore: 0.9,
            userCorrected: true,
            fmNegativeWithStrongCertainty: false,
            transcriptVersionChanged: false,
            cohortOnlyChange: false
        ))
    }

    @Test("user correction overrides cohort-only lock-in too")
    func userCorrectionOverridesCohortOnly() {
        #expect(policy.canRemoveCue(
            currentScore: 0.9,
            userCorrected: true,
            fmNegativeWithStrongCertainty: false,
            transcriptVersionChanged: false,
            cohortOnlyChange: true
        ))
    }

    // MARK: - Cohort-only changes

    @Test("cohort-only change: removes cue when score below suppressionThreshold")
    func cohortOnlyBelowSuppressionAllowsRemoval() {
        #expect(policy.canRemoveCue(
            currentScore: 0.20,  // below 0.25
            userCorrected: false,
            fmNegativeWithStrongCertainty: false,
            transcriptVersionChanged: false,
            cohortOnlyChange: true
        ))
    }

    @Test("cohort-only change: blocks removal when score above suppressionThreshold")
    func cohortOnlyAboveSuppressionBlocksRemoval() {
        #expect(!policy.canRemoveCue(
            currentScore: 0.30,  // above 0.25
            userCorrected: false,
            fmNegativeWithStrongCertainty: false,
            transcriptVersionChanged: false,
            cohortOnlyChange: true
        ))
    }

    // MARK: - Full removal (non-cohort-only)

    @Test("score above stayThreshold blocks removal even with counterevidence")
    func highScoreBlocksRemoval() {
        #expect(!policy.canRemoveCue(
            currentScore: 0.50,  // above 0.45
            userCorrected: false,
            fmNegativeWithStrongCertainty: true,
            transcriptVersionChanged: true,
            cohortOnlyChange: false
        ))
    }

    @Test("score below stayThreshold alone is insufficient — needs counterevidence")
    func lowScoreAloneIsInsufficient() {
        #expect(!policy.canRemoveCue(
            currentScore: 0.30,  // below 0.45
            userCorrected: false,
            fmNegativeWithStrongCertainty: false,
            transcriptVersionChanged: false,
            cohortOnlyChange: false
        ))
    }

    @Test("score below stayThreshold + FM negative allows removal")
    func lowScoreWithFMNegativeAllowsRemoval() {
        #expect(policy.canRemoveCue(
            currentScore: 0.30,
            userCorrected: false,
            fmNegativeWithStrongCertainty: true,
            transcriptVersionChanged: false,
            cohortOnlyChange: false
        ))
    }

    @Test("score below stayThreshold + transcript changed allows removal")
    func lowScoreWithTranscriptChangeAllowsRemoval() {
        #expect(policy.canRemoveCue(
            currentScore: 0.30,
            userCorrected: false,
            fmNegativeWithStrongCertainty: false,
            transcriptVersionChanged: true,
            cohortOnlyChange: false
        ))
    }

    @Test("default thresholds match spec: stayThreshold=0.45 suppressionThreshold=0.25")
    func defaultThresholdsMatchSpec() {
        #expect(DecisionStabilityPolicy.default.stayThreshold == 0.45)
        #expect(DecisionStabilityPolicy.default.suppressionThreshold == 0.25)
    }
}
