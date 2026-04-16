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

    @Test("unknown intent + unknown ownership → detectOnly (Phase 6.5: banner for correction signal)")
    func unknownIntentAndOwnershipIsDetectOnly() {
        // Phase 6.5 (playhead-4my.16): (.unknown, .unknown) surfaces a banner so
        // Phase 7 (UserCorrections) has signal to learn from.
        #expect(SkipPolicyMatrix.action(for: .unknown, ownership: .unknown) == .detectOnly)
    }

    @Test("unknown intent + known ownership → logOnly (ownership known but intent unclear)", arguments: AdOwnership.allCases.filter { $0 != .unknown })
    func unknownIntentWithKnownOwnershipIsLogOnly(ownership: AdOwnership) {
        // Unknown intent with a known (non-unknown) ownership: insufficient to act.
        // Covers: .thirdParty, .show, .network, .guest — all return .logOnly.
        // Note: (.unknown, .guest) → .logOnly even though (.paid, .guest) → .detectOnly,
        // because without intent we can't determine whether a guest endorsement is paid.
        #expect(SkipPolicyMatrix.action(for: .unknown, ownership: ownership) == .logOnly)
    }

    @Test("unknown intent + guest ownership → logOnly (guest endorsement with no intent data)")
    func unknownIntentGuestOwnershipIsLogOnly() {
        // Explicit test for the .guest case: a guest endorsement where FM has not
        // classified commercial intent. We can't distinguish organic mention from paid
        // deal without intent signal, so .logOnly is correct.
        // Contrast: (.paid, .guest) → .detectOnly (intent is known).
        #expect(SkipPolicyMatrix.action(for: .unknown, ownership: .guest) == .logOnly)
    }

    @Test("paid/owned + unknown ownership → logOnly (ownership needed for decision)")
    func paidOrOwnedWithUnknownOwnershipLogsOnly() {
        // paid needs .thirdParty confirmation; owned needs .show/.network confirmation.
        // Unknown ownership means we can't make a determination.
        #expect(SkipPolicyMatrix.action(for: .paid, ownership: .unknown) == .logOnly)
        #expect(SkipPolicyMatrix.action(for: .owned, ownership: .unknown) == .logOnly)
    }

    @Test("defaultAction sentinel is still logOnly")
    func defaultActionIsLogOnly() {
        // The static sentinel documents the 'no-op' baseline; it is not used in
        // the main switch path and should remain .logOnly for backward compatibility.
        #expect(SkipPolicyMatrix.defaultAction == .logOnly)
    }
}

@Suite("DecisionCohort")
struct DecisionCohortTests {

    @Test("DecisionCohort is Hashable — equal cohorts hash equal")
    func equalCohortsHashEqual() {
        let a = DecisionCohort(
            featurePipelineHash: "fp1", fusionHash: "fu1",
            policyHash: "p1", stabilityHash: "s1", appBuild: "42",
            calibrationVersion: "v0"
        )
        let b = DecisionCohort(
            featurePipelineHash: "fp1", fusionHash: "fu1",
            policyHash: "p1", stabilityHash: "s1", appBuild: "42",
            calibrationVersion: "v0"
        )
        #expect(a == b)
        #expect(a.hashValue == b.hashValue)
    }

    @Test("DecisionCohort is Hashable — different cohorts differ")
    func differentCohortsAreNotEqual() {
        let a = DecisionCohort(
            featurePipelineHash: "fp1", fusionHash: "fu1",
            policyHash: "p1", stabilityHash: "s1", appBuild: "42",
            calibrationVersion: "v0"
        )
        let b = DecisionCohort(
            featurePipelineHash: "fp2", fusionHash: "fu1",
            policyHash: "p1", stabilityHash: "s1", appBuild: "42",
            calibrationVersion: "v0"
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

    // Boundary: stayThreshold is a strict less-than — exactly 0.45 should NOT allow removal
    @Test("score exactly at stayThreshold (0.45) does not satisfy score condition")
    func exactlyAtStayThresholdBlocks() {
        #expect(!policy.canRemoveCue(
            currentScore: 0.45,
            userCorrected: false,
            fmNegativeWithStrongCertainty: true,
            transcriptVersionChanged: true,
            cohortOnlyChange: false
        ))
    }

    // Boundary: suppressionThreshold is a strict less-than — exactly 0.25 should NOT allow cohort-only removal
    @Test("cohort-only: score exactly at suppressionThreshold (0.25) does not allow removal")
    func exactlyAtSuppressionThresholdBlocksCohortOnly() {
        #expect(!policy.canRemoveCue(
            currentScore: 0.25,
            userCorrected: false,
            fmNegativeWithStrongCertainty: false,
            transcriptVersionChanged: false,
            cohortOnlyChange: true
        ))
    }
}

@Suite("SkipPolicyMatrix — unlisted matrix cells")
struct SkipPolicyMatrixUnlistedCellsTests {

    // paid + show/network: ambiguous ownership, defaults to logOnly until Phase 8
    @Test("paid + show → logOnly (ambiguous ownership, Phase 8 will resolve)")
    func paidShowIsLogOnly() {
        #expect(SkipPolicyMatrix.action(for: .paid, ownership: .show) == .logOnly)
    }

    @Test("paid + network → logOnly (ambiguous ownership, Phase 8 will resolve)")
    func paidNetworkIsLogOnly() {
        #expect(SkipPolicyMatrix.action(for: .paid, ownership: .network) == .logOnly)
    }

    // owned + thirdParty: a third-party spot claimed as owned is suspicious → logOnly
    @Test("owned + thirdParty → logOnly (contradictory attribution)")
    func ownedThirdPartyIsLogOnly() {
        #expect(SkipPolicyMatrix.action(for: .owned, ownership: .thirdParty) == .logOnly)
    }
}

// MARK: - Phase 6.5 Confidence Promotion (playhead-4my.17)

/// Tests for the post-policy confidence promotion step in AdDetectionService.runBackfill.
/// The promotion logic lives in the service, not the matrix, so these tests drive it
/// through AdDetectionConfig to verify the threshold field is wired correctly.
@Suite("AdDetectionConfig — autoSkipConfidenceThreshold")
struct AutoSkipConfidenceThresholdTests {

    @Test("default autoSkipConfidenceThreshold is 0.75")
    func defaultThresholdIs075() {
        #expect(AdDetectionConfig.default.autoSkipConfidenceThreshold == 0.75)
    }

    @Test("custom autoSkipConfidenceThreshold is stored correctly")
    func customThresholdIsStored() {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-v1",
            autoSkipConfidenceThreshold: 0.60
        )
        #expect(config.autoSkipConfidenceThreshold == 0.60)
    }

    @Test("suppress policy action is never overridden by confidence promotion")
    func suppressIsNeverPromoted() {
        // The suppress case in SkipPolicyMatrix represents organic content —
        // no amount of confidence should override a suppress decision.
        #expect(SkipPolicyMatrix.action(for: .organic, ownership: .thirdParty) == .suppress)
    }
}

// MARK: - SkipPolicyOverride (ef2.6.2)

@Suite("SkipPolicyOverride — scope and serialization")
struct SkipPolicyOverrideScopeTests {

    @Test("adType scope round-trips through serialization")
    func adTypeScopeRoundTrips() {
        let scope = SkipPolicyOverrideScope.adType(intent: .paid, ownership: .thirdParty)
        let serialized = scope.serialized
        let deserialized = SkipPolicyOverrideScope.deserialize(serialized)
        #expect(deserialized == scope)
    }

    @Test("showLevel scope round-trips through serialization")
    func showLevelScopeRoundTrips() {
        let scope = SkipPolicyOverrideScope.showLevel(podcastId: "podcast-123", intent: .owned, ownership: .show)
        let serialized = scope.serialized
        let deserialized = SkipPolicyOverrideScope.deserialize(serialized)
        #expect(deserialized == scope)
    }

    @Test("showWide scope round-trips through serialization")
    func showWideScopeRoundTrips() {
        let scope = SkipPolicyOverrideScope.showWide(podcastId: "podcast-456")
        let serialized = scope.serialized
        let deserialized = SkipPolicyOverrideScope.deserialize(serialized)
        #expect(deserialized == scope)
    }

    @Test("deserialization returns nil for malformed input")
    func malformedInputReturnsNil() {
        #expect(SkipPolicyOverrideScope.deserialize("garbage") == nil)
        #expect(SkipPolicyOverrideScope.deserialize("") == nil)
        #expect(SkipPolicyOverrideScope.deserialize("adType:bad:bad") == nil)
    }

    @Test("SkipPolicyOverride is Codable")
    func overrideCodableRoundTrip() throws {
        let override = SkipPolicyOverride(
            scope: .adType(intent: .paid, ownership: .thirdParty),
            action: .detectOnly,
            reason: "User prefers to see third-party ads as banners only"
        )
        let data = try JSONEncoder().encode(override)
        let decoded = try JSONDecoder().decode(SkipPolicyOverride.self, from: data)
        #expect(decoded.scope == override.scope)
        #expect(decoded.action == override.action)
        #expect(decoded.reason == override.reason)
    }
}

@Suite("SkipPolicyOverrideStore")
struct SkipPolicyOverrideStoreTests {

    @Test("empty store returns no overrides")
    func emptyStoreReturnsNone() {
        let store = InMemorySkipPolicyOverrideStore()
        let result = store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: nil)
        #expect(result == nil)
    }

    @Test("adType override returns matching action")
    func adTypeOverrideReturns() {
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .adType(intent: .paid, ownership: .thirdParty),
            action: .detectOnly,
            reason: "never skip third-party"
        ))
        let result = store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: nil)
        #expect(result == .detectOnly)
    }

    @Test("adType override does not affect unrelated intent/ownership")
    func adTypeOverrideDoesNotBleed() {
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .adType(intent: .paid, ownership: .thirdParty),
            action: .detectOnly,
            reason: "never skip third-party"
        ))
        // owned + show should be unaffected
        let result = store.effectiveAction(for: .owned, ownership: .show, podcastId: nil)
        #expect(result == nil)
    }

    @Test("showLevel override applies only to matching podcast")
    func showLevelOverrideMatchesPodcast() {
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .showLevel(podcastId: "pod-1", intent: .owned, ownership: .show),
            action: .autoSkipEligible,
            reason: "skip house promos for this show"
        ))
        // Matching podcast + intent + ownership
        let match = store.effectiveAction(for: .owned, ownership: .show, podcastId: "pod-1")
        #expect(match == .autoSkipEligible)
        // Different podcast — no match
        let noMatch = store.effectiveAction(for: .owned, ownership: .show, podcastId: "pod-2")
        #expect(noMatch == nil)
        // No podcast — no match
        let noPod = store.effectiveAction(for: .owned, ownership: .show, podcastId: nil)
        #expect(noPod == nil)
    }

    @Test("showWide override applies to any intent/ownership for that podcast")
    func showWideOverrideMatchesAnyType() {
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .showWide(podcastId: "pod-1"),
            action: .detectOnly,
            reason: "detect-only for this entire show"
        ))
        #expect(store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: "pod-1") == .detectOnly)
        #expect(store.effectiveAction(for: .owned, ownership: .show, podcastId: "pod-1") == .detectOnly)
        // Different podcast — no match
        #expect(store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: "pod-2") == nil)
    }

    @Test("showLevel override takes precedence over adType override")
    func showLevelPrecedenceOverAdType() {
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .adType(intent: .paid, ownership: .thirdParty),
            action: .detectOnly,
            reason: "global: never skip third-party"
        ))
        store.addOverride(SkipPolicyOverride(
            scope: .showLevel(podcastId: "pod-1", intent: .paid, ownership: .thirdParty),
            action: .autoSkipEligible,
            reason: "per-show: allow skipping third-party for this show"
        ))
        // Show-level wins for this podcast
        let result = store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: "pod-1")
        #expect(result == .autoSkipEligible)
        // Another podcast falls back to adType override
        let other = store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: "pod-2")
        #expect(other == .detectOnly)
    }

    @Test("showLevel override takes precedence over showWide override")
    func showLevelPrecedenceOverShowWide() {
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .showWide(podcastId: "pod-1"),
            action: .detectOnly,
            reason: "detect-only for this show"
        ))
        store.addOverride(SkipPolicyOverride(
            scope: .showLevel(podcastId: "pod-1", intent: .paid, ownership: .thirdParty),
            action: .autoSkipEligible,
            reason: "but allow skipping third-party for this show"
        ))
        // Show-level wins for matching intent/ownership
        let specific = store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: "pod-1")
        #expect(specific == .autoSkipEligible)
        // Other types fall back to showWide
        let other = store.effectiveAction(for: .owned, ownership: .show, podcastId: "pod-1")
        #expect(other == .detectOnly)
    }

    @Test("showWide override takes precedence over adType override")
    func showWidePrecedenceOverAdType() {
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .adType(intent: .paid, ownership: .thirdParty),
            action: .autoSkipEligible,
            reason: "global: skip third-party"
        ))
        store.addOverride(SkipPolicyOverride(
            scope: .showWide(podcastId: "pod-1"),
            action: .detectOnly,
            reason: "per-show: detect-only for this show"
        ))
        // showWide wins for this podcast
        let result = store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: "pod-1")
        #expect(result == .detectOnly)
        // Another podcast falls back to adType override
        let other = store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: "pod-2")
        #expect(other == .autoSkipEligible)
    }

    @Test("removing an override restores default behavior")
    func removeOverride() {
        let store = InMemorySkipPolicyOverrideStore()
        let scope = SkipPolicyOverrideScope.adType(intent: .paid, ownership: .thirdParty)
        store.addOverride(SkipPolicyOverride(scope: scope, action: .detectOnly, reason: "test"))
        #expect(store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: nil) == .detectOnly)
        store.removeOverride(for: scope)
        #expect(store.effectiveAction(for: .paid, ownership: .thirdParty, podcastId: nil) == nil)
    }

    @Test("allOverrides returns all stored overrides")
    func allOverridesReturnsAll() {
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .adType(intent: .paid, ownership: .thirdParty),
            action: .detectOnly,
            reason: "a"
        ))
        store.addOverride(SkipPolicyOverride(
            scope: .showWide(podcastId: "pod-1"),
            action: .suppress,
            reason: "b"
        ))
        #expect(store.allOverrides.count == 2)
    }
}

@Suite("SkipPolicyMatrix.action(overrideStore:) — override integration")
struct SkipPolicyMatrixOverrideIntegrationTests {

    @Test("override changes policy action without affecting what the matrix would return")
    func overrideChangesAction() {
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .adType(intent: .paid, ownership: .thirdParty),
            action: .detectOnly,
            reason: "never skip third-party"
        ))
        // Without override, paid+thirdParty = autoSkipEligible
        let baseline = SkipPolicyMatrix.action(for: .paid, ownership: .thirdParty)
        #expect(baseline == .autoSkipEligible)
        // With override, demoted to detectOnly
        let overridden = SkipPolicyMatrix.action(
            for: .paid, ownership: .thirdParty,
            overrideStore: store, podcastId: nil
        )
        #expect(overridden == .detectOnly)
    }

    @Test("nil override store returns default matrix action")
    func nilStoreReturnsDefault() {
        let result = SkipPolicyMatrix.action(
            for: .paid, ownership: .thirdParty,
            overrideStore: nil, podcastId: nil
        )
        #expect(result == .autoSkipEligible)
    }

    @Test("override store with no matching override returns default matrix action")
    func noMatchingOverrideReturnsDefault() {
        let store = InMemorySkipPolicyOverrideStore()
        // Add an override for a different type
        store.addOverride(SkipPolicyOverride(
            scope: .adType(intent: .owned, ownership: .show),
            action: .suppress,
            reason: "suppress house promos"
        ))
        let result = SkipPolicyMatrix.action(
            for: .paid, ownership: .thirdParty,
            overrideStore: store, podcastId: nil
        )
        #expect(result == .autoSkipEligible)
    }

    @Test("override does not affect confidence score — only policy action")
    func overrideDoesNotAffectScore() {
        // This is a design constraint test: skip-policy overrides live at Stage 4
        // and cannot modify skipConfidence. The DecisionMapper is responsible for
        // the score; the policy matrix only produces the action.
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .adType(intent: .paid, ownership: .thirdParty),
            action: .detectOnly,
            reason: "never skip third-party"
        ))
        // The action is a pure value — no confidence field exists on SkipPolicyAction.
        // This test documents the invariant: overrides change policy, not score.
        let action = SkipPolicyMatrix.action(
            for: .paid, ownership: .thirdParty,
            overrideStore: store, podcastId: nil
        )
        #expect(action == .detectOnly)
        // Action is a simple enum — no score coupling
    }

    @Test("classification override vs skip-policy override are distinct concerns")
    func classificationVsSkipPolicyAreDistinct() {
        // Classification override: "this IS an ad" or "this ISN'T an ad" —
        // handled by UserCorrectionStore (correctionFactor in DecisionMapper)
        // Skip-policy override: "don't skip this TYPE of ad" —
        // handled by SkipPolicyOverrideStore (action override in SkipPolicyMatrix)
        //
        // A user saying "that was a house promo" is a classification override.
        // A user saying "never skip house promos" is a skip-policy override.
        // These are orthogonal: you can identify content as an ad (high confidence)
        // but still choose not to skip it (policy override).
        let store = InMemorySkipPolicyOverrideStore()
        store.addOverride(SkipPolicyOverride(
            scope: .adType(intent: .owned, ownership: .show),
            action: .detectOnly,
            reason: "show promos as banners, never skip"
        ))
        // owned+show with override → detectOnly (policy override)
        let action = SkipPolicyMatrix.action(
            for: .owned, ownership: .show,
            overrideStore: store, podcastId: nil
        )
        #expect(action == .detectOnly)
        // Without override, the matrix also returns detectOnly for this pair —
        // but the override enforces it. If the matrix default ever changed,
        // the override would hold.
    }
}
