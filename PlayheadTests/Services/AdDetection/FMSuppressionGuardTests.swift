// FMSuppressionGuardTests.swift
// Tests for FMSuppressionGuard, FMSuppressionApplicator, and suppression integration.
//
// TDD: these tests specify the contract for targeted FM suppression (Phase ef2.4.6).

import Foundation
import Testing
@testable import Playhead

@Suite("FMSuppressionGuard")
struct FMSuppressionGuardTests {

    // MARK: - Helpers

    private func makeWeakLedger() -> [EvidenceLedgerEntry] {
        [
            EvidenceLedgerEntry(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            EvidenceLedgerEntry(source: .lexical, weight: 0.15, detail: .lexical(matchedCategories: ["transitionMarker"])),
            EvidenceLedgerEntry(source: .acoustic, weight: 0.10, detail: .acoustic(breakStrength: 0.5)),
        ]
    }

    private func makeStrongAnchorLedger() -> [EvidenceLedgerEntry] {
        [
            EvidenceLedgerEntry(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            EvidenceLedgerEntry(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["urlCTA"])),
        ]
    }

    private func makeFingerprintLedger() -> [EvidenceLedgerEntry] {
        [
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.6)),
            EvidenceLedgerEntry(source: .fingerprint, weight: 0.20, detail: .fingerprint(matchCount: 1, averageSimilarity: 0.9)),
        ]
    }

    private func makeLedgerWithFMContainsAd() -> [EvidenceLedgerEntry] {
        [
            EvidenceLedgerEntry(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.6)),
            EvidenceLedgerEntry(source: .lexical, weight: 0.10, detail: .lexical(matchedCategories: ["transitionMarker"])),
        ]
    }

    private func twoModerateNoAdsWindows() -> [FMSuppressionWindow] {
        [
            FMSuppressionWindow(disposition: .noAds, band: .moderate),
            FMSuppressionWindow(disposition: .noAds, band: .moderate),
        ]
    }

    private func twoStrongNoAdsWindows() -> [FMSuppressionWindow] {
        [
            FMSuppressionWindow(disposition: .noAds, band: .strong),
            FMSuppressionWindow(disposition: .noAds, band: .strong),
        ]
    }

    // MARK: - Guard evaluation: all guards pass

    @Test("Suppression triggers when all five guards pass")
    func allGuardsPass() {
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: twoModerateNoAdsWindows(),
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(result.isTriggered, "All guards pass: should trigger suppression")
    }

    @Test("Suppression triggers with strong-band noAds windows")
    func strongBandNoAdsTriggersGuard() {
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: twoStrongNoAdsWindows(),
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(result.isTriggered)
    }

    // MARK: - Guard 1: FM disposition must be noAds

    @Test("Suppression does NOT trigger when FM says containsAd")
    func containsAdDoesNotTrigger() {
        let windows = [
            FMSuppressionWindow(disposition: .containsAd, band: .strong),
            FMSuppressionWindow(disposition: .containsAd, band: .strong),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: windows,
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "containsAd should not trigger suppression")
    }

    @Test("Suppression does NOT trigger with uncertain disposition")
    func uncertainDoesNotTrigger() {
        let windows = [
            FMSuppressionWindow(disposition: .uncertain, band: .moderate),
            FMSuppressionWindow(disposition: .uncertain, band: .moderate),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: windows,
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "uncertain FM should not trigger suppression")
    }

    @Test("Suppression does NOT trigger with abstain disposition")
    func abstainDoesNotTrigger() {
        let windows = [
            FMSuppressionWindow(disposition: .abstain, band: .moderate),
            FMSuppressionWindow(disposition: .abstain, band: .moderate),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: windows,
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "abstain FM should not trigger suppression")
    }

    @Test("Suppression does NOT trigger with no FM results")
    func noFMResultsDoesNotTrigger() {
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: [],
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "no FM results should not trigger suppression")
    }

    // MARK: - Guard 2: CertaintyBand must be at least moderate

    @Test("Suppression does NOT trigger when noAds certainty is weak")
    func weakCertaintyDoesNotTrigger() {
        let windows = [
            FMSuppressionWindow(disposition: .noAds, band: .weak),
            FMSuppressionWindow(disposition: .noAds, band: .weak),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: windows,
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "weak certainty should not trigger suppression")
    }

    @Test("One weak and one moderate noAds: still only 1 moderate, does NOT trigger (need 2)")
    func oneWeakOneModerateDoesNotTrigger() {
        let windows = [
            FMSuppressionWindow(disposition: .noAds, band: .weak),
            FMSuppressionWindow(disposition: .noAds, band: .moderate),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: windows,
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "need 2+ moderate/strong noAds windows")
    }

    // MARK: - Guard 3: No strong anchors

    @Test("Suppression does NOT trigger when URL lexical anchor present")
    func urlAnchorBlocksSuppression() {
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: twoModerateNoAdsWindows(),
            ledger: makeStrongAnchorLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "URL anchor should block suppression")
    }

    @Test("Suppression does NOT trigger when promoCode lexical anchor present")
    func promoCodeAnchorBlocksSuppression() {
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.6)),
            EvidenceLedgerEntry(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["promoCode"])),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: twoModerateNoAdsWindows(),
            ledger: ledger,
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "promoCode anchor should block suppression")
    }

    @Test("Suppression does NOT trigger when sponsor lexical anchor present")
    func sponsorAnchorBlocksSuppression() {
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.6)),
            EvidenceLedgerEntry(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["sponsor"])),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: twoModerateNoAdsWindows(),
            ledger: ledger,
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "sponsor anchor should block suppression")
    }

    @Test("Suppression does NOT trigger when catalog entry present")
    func catalogEntryBlocksSuppression() {
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.6)),
            EvidenceLedgerEntry(source: .catalog, weight: 0.15, detail: .catalog(entryCount: 1)),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: twoModerateNoAdsWindows(),
            ledger: ledger,
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "catalog entry should block suppression")
    }

    @Test("Suppression does NOT trigger when evidenceCatalog anchor provenance present")
    func evidenceCatalogProvenanceBlocksSuppression() {
        let provenance: [AnchorRef] = [
            .evidenceCatalog(entry: EvidenceEntry(
                evidenceRef: 0,
                category: .url,
                matchedText: "example.com",
                normalizedText: "example.com",
                atomOrdinal: 100,
                startTime: 10.0,
                endTime: 11.0
            ))
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: twoModerateNoAdsWindows(),
            ledger: makeWeakLedger(),
            anchorProvenance: provenance
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "evidenceCatalog provenance should block suppression")
    }

    @Test("Weak lexical categories (transitionMarker, purchaseLanguage) do NOT block suppression")
    func weakLexicalCategoriesDoNotBlockSuppression() {
        let ledger = [
            EvidenceLedgerEntry(source: .lexical, weight: 0.15,
                                detail: .lexical(matchedCategories: ["transitionMarker", "purchaseLanguage"])),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: twoModerateNoAdsWindows(),
            ledger: ledger,
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(result.isTriggered, "weak lexical categories should NOT block suppression")
    }

    // MARK: - Guard 4: No fingerprint match

    @Test("Suppression does NOT trigger when fingerprint match present")
    func fingerprintBlocksSuppression() {
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: twoModerateNoAdsWindows(),
            ledger: makeFingerprintLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "fingerprint match should block suppression")
    }

    // MARK: - Guard 5: 2+ overlapping noAds windows

    @Test("Suppression does NOT trigger with only 1 noAds window")
    func singleWindowDoesNotTrigger() {
        let windows = [
            FMSuppressionWindow(disposition: .noAds, band: .strong),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: windows,
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(!result.isTriggered, "need 2+ noAds windows for consensus")
    }

    @Test("Suppression triggers with exactly 2 moderate noAds windows")
    func twoWindowsIsSufficient() {
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: twoModerateNoAdsWindows(),
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(result.isTriggered, "2 moderate noAds windows should be sufficient")
    }

    @Test("Suppression triggers with 3 moderate+ noAds windows")
    func threeWindowsTriggers() {
        let windows = [
            FMSuppressionWindow(disposition: .noAds, band: .moderate),
            FMSuppressionWindow(disposition: .noAds, band: .strong),
            FMSuppressionWindow(disposition: .noAds, band: .moderate),
        ]
        let guard_ = FMSuppressionGuard(
            overlappingFMResults: windows,
            ledger: makeWeakLedger(),
            anchorProvenance: []
        )
        let result = guard_.evaluate()
        #expect(result.isTriggered)
    }
}

// MARK: - FMSuppressionApplicator Tests

@Suite("FMSuppressionApplicator")
struct FMSuppressionApplicatorTests {

    // MARK: - Helpers

    private func makeWeakLedger() -> [EvidenceLedgerEntry] {
        [
            EvidenceLedgerEntry(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            EvidenceLedgerEntry(source: .lexical, weight: 0.15, detail: .lexical(matchedCategories: ["transitionMarker"])),
            EvidenceLedgerEntry(source: .acoustic, weight: 0.10, detail: .acoustic(breakStrength: 0.5)),
        ]
    }

    // MARK: - Not triggered: ledger unchanged

    @Test("Applicator returns original ledger when guard not triggered")
    func notTriggeredPreservesLedger() {
        let ledger = makeWeakLedger()
        let applicator = FMSuppressionApplicator()
        let result = applicator.apply(
            guardResult: .notTriggered(reason: "test"),
            ledger: ledger
        )
        #expect(!result.applied)
        #expect(result.downweightedCount == 0)
        #expect(!result.cappedToMarkOnly)
        #expect(result.suppressedLedger.count == ledger.count)
        // Weights unchanged
        for (original, suppressed) in zip(ledger, result.suppressedLedger) {
            #expect(original.weight == suppressed.weight)
        }
    }

    // MARK: - Triggered: weak evidence downweighted

    @Test("Applicator downweights weak evidence when triggered")
    func triggeredDownweightsWeakEvidence() {
        let ledger = makeWeakLedger()
        let applicator = FMSuppressionApplicator(suppressionFactor: 0.3)
        let result = applicator.apply(
            guardResult: .triggered,
            ledger: ledger
        )
        #expect(result.applied)
        #expect(result.downweightedCount == 3, "All 3 entries are weak and should be downweighted")

        // Each weight should be original * 0.3
        #expect(abs(result.suppressedLedger[0].weight - 0.25 * 0.3) < 0.001)
        #expect(abs(result.suppressedLedger[1].weight - 0.15 * 0.3) < 0.001)
        #expect(abs(result.suppressedLedger[2].weight - 0.10 * 0.3) < 0.001)
    }

    @Test("Weak evidence is downweighted, not removed")
    func downweightedNotRemoved() {
        let ledger = makeWeakLedger()
        let applicator = FMSuppressionApplicator()
        let result = applicator.apply(guardResult: .triggered, ledger: ledger)
        #expect(result.suppressedLedger.count == ledger.count,
                "Entry count must not change — downweight, not removal")
    }

    // MARK: - Strong evidence preserved

    @Test("FM containsAd entries are preserved during suppression")
    func fmContainsAdPreserved() {
        let ledger = [
            EvidenceLedgerEntry(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.6)),
        ]
        let applicator = FMSuppressionApplicator(suppressionFactor: 0.3)
        let result = applicator.apply(guardResult: .triggered, ledger: ledger)

        let fmEntry = result.suppressedLedger.first { $0.source == .fm }!
        #expect(fmEntry.weight == 0.35, "FM containsAd entry weight must be preserved")
        #expect(result.downweightedCount == 1, "Only classifier is weak here")
    }

    @Test("Fingerprint entries are preserved during suppression")
    func fingerprintPreserved() {
        let ledger = [
            EvidenceLedgerEntry(source: .fingerprint, weight: 0.20, detail: .fingerprint(matchCount: 1, averageSimilarity: 0.9)),
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.6)),
        ]
        let applicator = FMSuppressionApplicator(suppressionFactor: 0.3)
        let result = applicator.apply(guardResult: .triggered, ledger: ledger)

        let fpEntry = result.suppressedLedger.first { $0.source == .fingerprint }!
        #expect(fpEntry.weight == 0.20, "Fingerprint entry weight must be preserved")
        #expect(result.downweightedCount == 1)
    }

    // MARK: - cappedToMarkOnly behavior

    @Test("cappedToMarkOnly is true when no strong proposal survives suppression")
    func cappedToMarkOnlyWhenNoStrongProposal() {
        let ledger = makeWeakLedger()
        let applicator = FMSuppressionApplicator()
        let result = applicator.apply(guardResult: .triggered, ledger: ledger)
        #expect(result.cappedToMarkOnly, "No strong evidence = capped to markOnly")
    }

    @Test("cappedToMarkOnly is false when FM containsAd survives")
    func notCappedWhenFMContainsAdSurvives() {
        let ledger = [
            EvidenceLedgerEntry(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.6)),
        ]
        let applicator = FMSuppressionApplicator()
        let result = applicator.apply(guardResult: .triggered, ledger: ledger)
        #expect(!result.cappedToMarkOnly, "FM containsAd is strong, should not cap")
    }

    @Test("cappedToMarkOnly is false when fingerprint survives")
    func notCappedWhenFingerprintSurvives() {
        let ledger = [
            EvidenceLedgerEntry(source: .fingerprint, weight: 0.20, detail: .fingerprint(matchCount: 1, averageSimilarity: 0.9)),
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.6)),
        ]
        let applicator = FMSuppressionApplicator()
        let result = applicator.apply(guardResult: .triggered, ledger: ledger)
        #expect(!result.cappedToMarkOnly, "Fingerprint is strong, should not cap")
    }

    // MARK: - Custom suppression factor

    @Test("Custom suppressionFactor is applied correctly")
    func customSuppressionFactor() {
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.6)),
        ]
        let applicator = FMSuppressionApplicator(suppressionFactor: 0.5)
        let result = applicator.apply(guardResult: .triggered, ledger: ledger)
        #expect(abs(result.suppressedLedger[0].weight - 0.10) < 0.001,
                "0.20 * 0.5 = 0.10")
    }

    // MARK: - Suppression result attribution

    @Test("Suppression result contains attribution details")
    func suppressionResultAttribution() {
        let ledger = makeWeakLedger()
        let applicator = FMSuppressionApplicator(suppressionFactor: 0.3)
        let result = applicator.apply(guardResult: .triggered, ledger: ledger)
        #expect(result.applied)
        #expect(result.reason.contains("noAds consensus"), "Reason should mention noAds consensus")
        #expect(result.reason.contains("0.3"), "Reason should mention the factor")
        #expect(result.downweightedCount == 3)
    }

    @Test("Not-triggered result contains reason")
    func notTriggeredResultAttribution() {
        let applicator = FMSuppressionApplicator()
        let result = applicator.apply(
            guardResult: .notTriggered(reason: "no noAds FM disposition"),
            ledger: []
        )
        #expect(!result.applied)
        #expect(result.reason.contains("no noAds FM disposition"))
    }
}

// MARK: - CertaintyBand extension tests

@Suite("CertaintyBand.isAtLeastModerate")
struct CertaintyBandExtensionTests {

    @Test("weak is NOT at least moderate")
    func weakIsNotModerate() {
        #expect(!CertaintyBand.weak.isAtLeastModerate)
    }

    @Test("moderate IS at least moderate")
    func moderateIsModerate() {
        #expect(CertaintyBand.moderate.isAtLeastModerate)
    }

    @Test("strong IS at least moderate")
    func strongIsAtLeastModerate() {
        #expect(CertaintyBand.strong.isAtLeastModerate)
    }
}

// MARK: - SkipEligibilityGate: cappedByFMSuppression

@Suite("SkipEligibilityGate.cappedByFMSuppression")
struct CappedByFMSuppressionGateTests {

    @Test("cappedByFMSuppression is Codable round-trippable")
    func codableRoundTrip() throws {
        let gate = SkipEligibilityGate.cappedByFMSuppression
        let data = try JSONEncoder().encode(gate)
        let decoded = try JSONDecoder().decode(SkipEligibilityGate.self, from: data)
        #expect(decoded == gate)
    }

    @Test("cappedByFMSuppression rawValue is correct")
    func rawValue() {
        #expect(SkipEligibilityGate.cappedByFMSuppression.rawValue == "cappedByFMSuppression")
    }

    // MARK: - classificationTrust preservation

    @Test("Suppression preserves classificationTrust on downweighted entries")
    func classificationTrustPreservedThroughSuppression() {
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.25,
                                detail: .classifier(score: 0.7), classificationTrust: 0.85),
            EvidenceLedgerEntry(source: .lexical, weight: 0.15,
                                detail: .lexical(matchedCategories: ["transitionMarker"]),
                                classificationTrust: 0.6),
        ]
        let applicator = FMSuppressionApplicator(suppressionFactor: 0.3)
        let result = applicator.apply(guardResult: .triggered, ledger: ledger)

        #expect(result.suppressedLedger[0].classificationTrust == 0.85,
                "classificationTrust must be forwarded, not reset to default")
        #expect(result.suppressedLedger[1].classificationTrust == 0.6,
                "classificationTrust must be forwarded, not reset to default")
    }

    @Test("subSource is preserved across suppression downweighting (playhead-rfu-sad)")
    func subSourcePreservedThroughSuppression() {
        // Catalog entries can carry a `subSource` disambiguator
        // (`.transcriptCatalog` vs `.fingerprintStore`). Before
        // playhead-rfu-sad the applicator rebuilt entries via the
        // 4-arg init and silently dropped the field, collapsing the
        // two distinct provenance buckets after suppression. NARL
        // replay attributes by `subSource`, so dropping it would
        // make cross-episode fingerprint matches and per-episode
        // transcript matches indistinguishable downstream.
        let ledger = [
            EvidenceLedgerEntry(
                source: .catalog,
                weight: 0.20,
                detail: .catalog(entryCount: 3),
                classificationTrust: 1.0,
                subSource: .transcriptCatalog
            ),
            EvidenceLedgerEntry(
                source: .catalog,
                weight: 0.18,
                detail: .catalog(entryCount: 2),
                classificationTrust: 1.0,
                subSource: .fingerprintStore
            ),
        ]
        let applicator = FMSuppressionApplicator(suppressionFactor: 0.3)
        let result = applicator.apply(guardResult: .triggered, ledger: ledger)

        #expect(result.applied)
        #expect(result.suppressedLedger.count == 2)
        // Both entries are weak (.catalog) → both should be downweighted,
        // and both should keep their distinct subSource labels.
        #expect(result.suppressedLedger[0].subSource == .transcriptCatalog,
                "transcriptCatalog subSource must round-trip through suppression")
        #expect(result.suppressedLedger[1].subSource == .fingerprintStore,
                "fingerprintStore subSource must round-trip through suppression")
        #expect(abs(result.suppressedLedger[0].weight - 0.20 * 0.3) < 0.001)
        #expect(abs(result.suppressedLedger[1].weight - 0.18 * 0.3) < 0.001)
    }
}
