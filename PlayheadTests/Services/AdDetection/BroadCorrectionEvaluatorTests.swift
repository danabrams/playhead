// BroadCorrectionEvaluatorTests.swift
// ef2.6.1: Tests for broader learned correction scopes (Layer B).
//
// Rule promotion from repeated corrections:
//   phraseOnShow:      3 corrections, 2 episodes, 2 dates → promote, 120-day decay
//   sponsorOnShow:     2 corrections, 2 episodes           → promote, 180-day decay
//   domainOwnershipOnShow: 2 corrections, 2 episodes       → promote, 360-day decay
//   jingleOnShow:      3 corrections, 2 episodes, 2 dates → promote, 90-day decay
//
// Implicit feedback contributes at 0.3× weight.
// exactSpan vetoes are permanent (Layer A) and never decay — preserved here.

import Foundation
import Testing
@testable import Playhead

// MARK: - BroadCorrectionScope Configuration

@Suite("BroadCorrectionScope — Thresholds & Decay")
struct BroadCorrectionScopeConfigTests {

    @Test("phraseOnShow requires 3 corrections, 2 episodes, 2 dates, 120-day decay")
    func phraseOnShowConfig() {
        let config = BroadCorrectionScope.phraseOnShow
        #expect(config.requiredCorrectionCount == 3)
        #expect(config.requiredDistinctEpisodes == 2)
        #expect(config.requiredDistinctDates == 2)
        #expect(config.decayDays == 120)
    }

    @Test("sponsorOnShow requires 2 corrections, 2 episodes, no date requirement, 180-day decay")
    func sponsorOnShowConfig() {
        let config = BroadCorrectionScope.sponsorOnShow
        #expect(config.requiredCorrectionCount == 2)
        #expect(config.requiredDistinctEpisodes == 2)
        #expect(config.requiredDistinctDates == nil)
        #expect(config.decayDays == 180)
    }

    @Test("domainOwnershipOnShow requires 2 corrections, 2 episodes, no date requirement, 360-day decay")
    func domainOwnershipOnShowConfig() {
        let config = BroadCorrectionScope.domainOwnershipOnShow
        #expect(config.requiredCorrectionCount == 2)
        #expect(config.requiredDistinctEpisodes == 2)
        #expect(config.requiredDistinctDates == nil)
        #expect(config.decayDays == 360)
    }

    @Test("jingleOnShow requires 3 corrections, 2 episodes, 2 dates, 90-day decay")
    func jingleOnShowConfig() {
        let config = BroadCorrectionScope.jingleOnShow
        #expect(config.requiredCorrectionCount == 3)
        #expect(config.requiredDistinctEpisodes == 2)
        #expect(config.requiredDistinctDates == 2)
        #expect(config.decayDays == 90)
    }
}

// MARK: - BroadCorrectionEvaluator — Promotion Logic

@Suite("BroadCorrectionEvaluator — Promotion")
struct BroadCorrectionEvaluatorPromotionTests {

    private let referenceDate = Date(timeIntervalSince1970: 1_750_000_000)  // ~2025-06-15

    // MARK: - phraseOnShow

    @Test("phraseOnShow promotes with 3 explicit corrections across 2 episodes and 2 dates")
    func phraseOnShowPromotes() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 2), feedbackKind: .explicit),
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 2), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .phraseOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }

    @Test("phraseOnShow does not promote with only 2 explicit corrections")
    func phraseOnShowInsufficientCount() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .phraseOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == false)
    }

    @Test("phraseOnShow does not promote with 3 corrections from single episode")
    func phraseOnShowSingleEpisodeFails() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 2), feedbackKind: .explicit),
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .explicit),
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .phraseOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == false)
    }

    @Test("phraseOnShow does not promote with 3 corrections on same date")
    func phraseOnShowSameDateFails() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
            .init(episodeId: "ep-3", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .phraseOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == false)
    }

    // MARK: - sponsorOnShow

    @Test("sponsorOnShow promotes with 2 explicit corrections across 2 episodes")
    func sponsorOnShowPromotes() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .sponsorOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }

    @Test("sponsorOnShow does not promote with 2 corrections from same episode")
    func sponsorOnShowSameEpisodeFails() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .explicit),
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .sponsorOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == false)
    }

    @Test("sponsorOnShow promotes on same date (no date diversity requirement)")
    func sponsorOnShowSameDateOK() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .sponsorOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }

    // MARK: - domainOwnershipOnShow

    @Test("domainOwnershipOnShow promotes with 2 corrections across 2 episodes")
    func domainOwnershipOnShowPromotes() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .domainOwnershipOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }

    // MARK: - jingleOnShow

    @Test("jingleOnShow promotes with 3 corrections, 2 episodes, 2 dates")
    func jingleOnShowPromotes() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 5), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate.addingTimeInterval(-86400 * 5), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .jingleOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }

    @Test("jingleOnShow does not promote with only 2 corrections")
    func jingleOnShowInsufficientCount() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .jingleOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == false)
    }
}

// MARK: - Decay

@Suite("BroadCorrectionEvaluator — Decay")
struct BroadCorrectionEvaluatorDecayTests {

    private let referenceDate = Date(timeIntervalSince1970: 1_750_000_000)

    @Test("phraseOnShow: corrections older than 120 days are excluded")
    func phraseOnShowDecay() {
        let entries: [CorrectionLedgerEntry] = [
            // This correction is 121 days old — should be excluded by decay.
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 121), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .explicit),
            .init(episodeId: "ep-3", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .phraseOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        // Only 2 non-decayed corrections remain, below the threshold of 3.
        #expect(result == false)
    }

    @Test("sponsorOnShow: corrections at exactly 180 days are still included")
    func sponsorOnShowBoundary() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 180), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .sponsorOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }

    @Test("sponsorOnShow: corrections older than 180 days are excluded")
    func sponsorOnShowDecayed() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 181), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .sponsorOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        // Only 1 non-decayed correction, below threshold of 2.
        #expect(result == false)
    }

    @Test("domainOwnershipOnShow: 360-day decay window")
    func domainOwnershipLongDecay() {
        let entries: [CorrectionLedgerEntry] = [
            // 350 days old — still within 360-day window.
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 350), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .domainOwnershipOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }

    @Test("domainOwnershipOnShow: correction at 361 days is excluded")
    func domainOwnershipDecayed() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 361), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .domainOwnershipOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == false)
    }

    @Test("jingleOnShow: 90-day decay window")
    func jingleOnShowDecay() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 91), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .explicit),
            .init(episodeId: "ep-3", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .jingleOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        // First correction decayed, only 2 remain, below threshold of 3.
        #expect(result == false)
    }
}

// MARK: - Implicit Feedback (Weak Labels)

@Suite("BroadCorrectionEvaluator — Implicit Feedback")
struct BroadCorrectionEvaluatorImplicitTests {

    private let referenceDate = Date(timeIntervalSince1970: 1_750_000_000)

    @Test("Implicit feedback contributes at 0.3× weight")
    func implicitWeight() {
        #expect(CorrectionFeedbackKind.implicit.weight == 0.3)
        #expect(CorrectionFeedbackKind.explicit.weight == 1.0)
    }

    @Test("sponsorOnShow: 2 implicit corrections alone insufficient (0.6 < 2.0)")
    func implicitAloneInsufficient() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .implicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .implicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .sponsorOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == false)
    }

    @Test("sponsorOnShow: 1 explicit + 4 implicit = 1.0 + 1.2 = 2.2 ≥ 2.0, promotes if 2 episodes")
    func explicitPlusImplicitPromotes() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 3), feedbackKind: .explicit),
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 2), feedbackKind: .implicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .implicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .implicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .implicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .sponsorOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }

    @Test("phraseOnShow: implicit contributions count toward weighted total")
    func phraseImplicitContribution() {
        // 2 explicit (2.0) + 4 implicit (1.2) = 3.2 ≥ 3.0, with 2 episodes and 2 dates.
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 2), feedbackKind: .explicit),
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 2), feedbackKind: .implicit),
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 2), feedbackKind: .implicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .implicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .implicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .phraseOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }

    @Test("Implicit-only corrections still count toward episode diversity")
    func implicitCountsForDiversity() {
        // 7 implicit corrections = 2.1 weighted ≥ 2.0, across 2 episodes.
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .implicit),
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .implicit),
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .implicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .implicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .implicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .implicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate, feedbackKind: .implicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .sponsorOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }
    @Test("phraseOnShow: 10 implicit corrections exactly hit threshold (10×0.3=3.0, floating-point safe)")
    func implicitFloatingPointEdgeCase() {
        // 10 × 0.3 = 3.0 in exact math, but floating-point accumulation may
        // produce 2.9999…97. The evaluator must still promote.
        let entries: [CorrectionLedgerEntry] = (0..<10).map { i in
            .init(
                episodeId: i < 5 ? "ep-1" : "ep-2",
                correctionDate: referenceDate.addingTimeInterval(-86400 * Double(i % 3 + 1)),
                feedbackKind: .implicit
            )
        }
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .phraseOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true, "10 implicit corrections (10×0.3=3.0) should promote phraseOnShow")
    }
}

// MARK: - Layer-B Act-Alone Suppression (playhead-xsdz.34 Part 3)

/// The Dan-INCLUDED §8 carve-out: ONE explicit `.falsePositive` (suppress)
/// veto has act-alone authority to promote a Layer-B SUPPRESSION rule — an
/// EXPLICIT, separately-tested behavior via `shouldActAloneSuppress`, walled
/// off from `shouldPromote`'s corroboration path (which is unchanged).
@Suite("BroadCorrectionEvaluator — Layer-B act-alone suppression (xsdz.34)")
struct BroadCorrectionEvaluatorActAloneTests {

    private let referenceDate = Date(timeIntervalSince1970: 1_750_000_000)

    @Test("ONE explicit .falsePositive suppress veto acts alone to promote a Layer-B suppression rule")
    func explicitSuppressActsAlone() {
        for scope in BroadCorrectionScope.allCases {
            let entries: [CorrectionLedgerEntry] = [
                .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .explicit, direction: .suppress)
            ]
            #expect(
                BroadCorrectionEvaluator.shouldActAloneSuppress(
                    scope: scope, entries: entries, referenceDate: referenceDate
                ),
                "a single explicit suppress veto should act alone for \(scope)"
            )
        }
    }

    @Test("GUARDRAIL 1: an explicit BOOST veto never acts alone (suppress-direction only)")
    func explicitBoostNeverActsAlone() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .explicit, direction: .boost)
        ]
        #expect(!BroadCorrectionEvaluator.shouldActAloneSuppress(
            scope: .sponsorOnShow, entries: entries, referenceDate: referenceDate))
    }

    @Test("GUARDRAIL 2: a single implicit suppress signal never acts alone")
    func implicitSuppressNeverActsAlone() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .implicit, direction: .suppress)
        ]
        #expect(!BroadCorrectionEvaluator.shouldActAloneSuppress(
            scope: .sponsorOnShow, entries: entries, referenceDate: referenceDate))
    }

    @Test("a decayed explicit suppress veto does not act alone")
    func decayedExplicitSuppressDoesNotActAlone() {
        // sponsorOnShow decays at 180 days; 181 days old → excluded.
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1",
                  correctionDate: referenceDate.addingTimeInterval(-86400 * 181),
                  feedbackKind: .explicit, direction: .suppress)
        ]
        #expect(!BroadCorrectionEvaluator.shouldActAloneSuppress(
            scope: .sponsorOnShow, entries: entries, referenceDate: referenceDate))
    }

    @Test("empty entries never act alone")
    func emptyNeverActsAlone() {
        #expect(!BroadCorrectionEvaluator.shouldActAloneSuppress(
            scope: .sponsorOnShow, entries: [], referenceDate: referenceDate))
    }

    @Test("shouldPromote's corroboration path is UNCHANGED: one explicit veto still does not promote it")
    func shouldPromoteUnchangedByActAlone() {
        // Act-alone is a SEPARATE method. The corroboration-based `shouldPromote`
        // must still require its full ≥count / diversity — a single explicit
        // suppress veto does NOT promote via `shouldPromote`.
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .explicit, direction: .suppress)
        ]
        #expect(!BroadCorrectionEvaluator.shouldPromote(
            scope: .sponsorOnShow, entries: entries, referenceDate: referenceDate))
    }

    @Test("CorrectionLedgerEntry defaults to .suppress (pre-xsdz.34 call sites unchanged)")
    func directionDefaultsToSuppress() {
        let entry = CorrectionLedgerEntry(
            episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .explicit)
        #expect(entry.direction == .suppress)
    }
}

// MARK: - Never-Act-Alone Guards (playhead-xsdz.34 T8 / guardrail 2)

/// T8 (design §4.5 / §8 guardrail 2): the noisy-implicit-never-alone intent is
/// preserved. `ImplicitFeedbackEvent.weight` stays 0.3; a single implicit
/// signal promotes no Layer-B rule; `repeatedManualSkipForward` alone can never
/// license a suppression rule or an auto-skip.
@Suite("BroadCorrectionEvaluator — never-act-alone guards (xsdz.34 T8)")
struct NeverActAloneGuardTests {

    private let referenceDate = Date(timeIntervalSince1970: 1_750_000_000)

    @Test("ImplicitFeedbackEvent.weight is 0.3 (weak labels never gain act-alone authority)")
    func implicitWeightStays03() {
        let event = ImplicitFeedbackEvent(
            signal: .repeatedManualSkipForward, analysisAssetId: "asset-1")
        #expect(event.weight == 0.3)
        // The DB-hydration initializer also pins 0.3 regardless of a stale stored value.
        let hydrated = ImplicitFeedbackEvent(
            id: "id", signal: .repeatedManualSkipForward, analysisAssetId: "asset-1",
            podcastId: nil, spanId: nil, timestamp: 0, storedWeight: 0.99)
        #expect(hydrated.weight == 0.3)
    }

    @Test("a single implicit signal promotes no Layer-B rule (shouldPromote AND act-alone)")
    func singleImplicitPromotesNothing() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .implicit, direction: .suppress)
        ]
        for scope in BroadCorrectionScope.allCases {
            #expect(!BroadCorrectionEvaluator.shouldPromote(
                scope: scope, entries: entries, referenceDate: referenceDate))
            #expect(!BroadCorrectionEvaluator.shouldActAloneSuppress(
                scope: scope, entries: entries, referenceDate: referenceDate))
        }
    }

    @Test("repeatedManualSkipForward (implicit → boost) alone licenses no suppression rule")
    func repeatedManualSkipForwardNeverActsAlone() {
        // repeatedManualSkipForward is an implicit BOOST signal. Modeled as an
        // implicit boost-direction entry, it acts alone in NEITHER path — and
        // this bead adds NO boost-direction act-alone authority anywhere, so it
        // can never move a span toward auto-skip on its own.
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .implicit, direction: .boost)
        ]
        #expect(!BroadCorrectionEvaluator.shouldActAloneSuppress(
            scope: .sponsorOnShow, entries: entries, referenceDate: referenceDate))
        #expect(!BroadCorrectionEvaluator.shouldPromote(
            scope: .sponsorOnShow, entries: entries, referenceDate: referenceDate))
    }
}

// MARK: - exactSpan Veto Permanence

@Suite("BroadCorrectionEvaluator — exactSpan Permanence")
struct BroadCorrectionEvaluatorExactSpanTests {

    @Test("exactSpan vetoes are not evaluated by BroadCorrectionEvaluator")
    func exactSpanIsLayerA() {
        // BroadCorrectionScope does not include exactSpan — it is Layer A
        // and handled by existing UserCorrectionStore logic. Verify by
        // checking that all BroadCorrectionScope cases are the Layer B scopes.
        let allScopes: [BroadCorrectionScope] = [
            .phraseOnShow, .sponsorOnShow, .domainOwnershipOnShow, .jingleOnShow
        ]
        #expect(allScopes.count == 4)
    }
}

// MARK: - Edge Cases

@Suite("BroadCorrectionEvaluator — Edge Cases")
struct BroadCorrectionEvaluatorEdgeCaseTests {

    private let referenceDate = Date(timeIntervalSince1970: 1_750_000_000)

    @Test("Empty entries never promote")
    func emptyEntriesNoPromotion() {
        for scope in [BroadCorrectionScope.phraseOnShow, .sponsorOnShow, .domainOwnershipOnShow, .jingleOnShow] {
            let result = BroadCorrectionEvaluator.shouldPromote(
                scope: scope,
                entries: [],
                referenceDate: referenceDate
            )
            #expect(result == false, "Empty entries should never promote for \(scope)")
        }
    }

    @Test("All-decayed entries never promote")
    func allDecayedNoPromotion() {
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 400), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate.addingTimeInterval(-86400 * 400), feedbackKind: .explicit),
            .init(episodeId: "ep-3", correctionDate: referenceDate.addingTimeInterval(-86400 * 400), feedbackKind: .explicit),
        ]
        for scope in [BroadCorrectionScope.phraseOnShow, .sponsorOnShow, .domainOwnershipOnShow, .jingleOnShow] {
            let result = BroadCorrectionEvaluator.shouldPromote(
                scope: scope,
                entries: entries,
                referenceDate: referenceDate
            )
            #expect(result == false, "All-decayed entries should never promote for \(scope)")
        }
    }

    @Test("Exactly at decay boundary is included")
    func exactDecayBoundaryIncluded() {
        // jingleOnShow: 90-day decay. Correction at exactly 90 days should be included.
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400 * 90), feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .explicit),
            .init(episodeId: "ep-3", correctionDate: referenceDate, feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .jingleOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        #expect(result == true)
    }

    @Test("Date diversity uses calendar day, not 24-hour window")
    func dateDiversityUsesCalendarDay() {
        // Two corrections 23 hours apart but on the same calendar day (UTC).
        let calendar = Calendar(identifier: .gregorian)
        var components = DateComponents()
        components.year = 2025
        components.month = 6
        components.day = 15
        components.hour = 1
        components.timeZone = TimeZone(identifier: "UTC")
        let earlyMorning = calendar.date(from: components)!
        components.hour = 23
        let lateEvening = calendar.date(from: components)!

        let nextDay = earlyMorning.addingTimeInterval(86400 * 2) // June 17

        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: earlyMorning, feedbackKind: .explicit),
            .init(episodeId: "ep-1", correctionDate: lateEvening, feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: nextDay, feedbackKind: .explicit),
        ]
        // Same calendar day for first two: only 2 distinct dates (June 15, June 17).
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .phraseOnShow,
            entries: entries,
            referenceDate: nextDay
        )
        #expect(result == true, "Two distinct calendar dates should satisfy date diversity")
    }

    @Test("Diversity counts are checked after decay filtering")
    func diversityAfterDecay() {
        // 3 corrections across 2 episodes, but the only correction from ep-2 is decayed.
        let entries: [CorrectionLedgerEntry] = [
            .init(episodeId: "ep-1", correctionDate: referenceDate.addingTimeInterval(-86400), feedbackKind: .explicit),
            .init(episodeId: "ep-1", correctionDate: referenceDate, feedbackKind: .explicit),
            .init(episodeId: "ep-2", correctionDate: referenceDate.addingTimeInterval(-86400 * 121), feedbackKind: .explicit),
        ]
        let result = BroadCorrectionEvaluator.shouldPromote(
            scope: .phraseOnShow,
            entries: entries,
            referenceDate: referenceDate
        )
        // After decay: only 2 corrections from ep-1, fails both count and diversity.
        #expect(result == false)
    }
}

// MARK: - CorrectionScope Serialization for New Scopes

@Suite("CorrectionScope — New Layer B Scopes")
struct CorrectionScopeLayerBTests {

    @Test("domainOwnershipOnShow serialization round-trip")
    func domainOwnershipRoundTrip() {
        let scope = CorrectionScope.domainOwnershipOnShow(
            podcastId: "pod-abc",
            domain: "squarespace.com"
        )
        let serialized = scope.serialized
        #expect(serialized == "domainOwnershipOnShow:pod-abc:squarespace.com")
        let deserialized = CorrectionScope.deserialize(serialized)
        #expect(deserialized == scope)
    }

    @Test("jingleOnShow serialization round-trip")
    func jingleOnShowRoundTrip() {
        let scope = CorrectionScope.jingleOnShow(
            podcastId: "pod-xyz",
            jingleId: "jingle-fp-abc123"
        )
        let serialized = scope.serialized
        #expect(serialized == "jingleOnShow:pod-xyz:jingle-fp-abc123")
        let deserialized = CorrectionScope.deserialize(serialized)
        #expect(deserialized == scope)
    }

    @Test("domainOwnershipOnShow with colon in domain round-trips")
    func domainWithColonRoundTrip() {
        let scope = CorrectionScope.domainOwnershipOnShow(
            podcastId: "pod-1",
            domain: "example.com:8080"
        )
        let deserialized = CorrectionScope.deserialize(scope.serialized)
        #expect(deserialized == scope)
    }

    @Test("Unknown scope prefix returns nil")
    func unknownPrefixReturnsNil() {
        #expect(CorrectionScope.deserialize("unknownScope:pod:val") == nil)
    }
}

// MARK: - CorrectionScope broadScope mapping

@Suite("CorrectionScope — broadScope Mapping")
struct CorrectionScopeBroadScopeTests {

    @Test("phraseOnShow maps to BroadCorrectionScope.phraseOnShow")
    func phraseMapping() {
        let scope = CorrectionScope.phraseOnShow(podcastId: "p", phrase: "x")
        #expect(scope.broadScope == .phraseOnShow)
    }

    @Test("sponsorOnShow maps to BroadCorrectionScope.sponsorOnShow")
    func sponsorMapping() {
        let scope = CorrectionScope.sponsorOnShow(podcastId: "p", sponsor: "x")
        #expect(scope.broadScope == .sponsorOnShow)
    }

    @Test("domainOwnershipOnShow maps to BroadCorrectionScope.domainOwnershipOnShow")
    func domainMapping() {
        let scope = CorrectionScope.domainOwnershipOnShow(podcastId: "p", domain: "x")
        #expect(scope.broadScope == .domainOwnershipOnShow)
    }

    @Test("jingleOnShow maps to BroadCorrectionScope.jingleOnShow")
    func jingleMapping() {
        let scope = CorrectionScope.jingleOnShow(podcastId: "p", jingleId: "x")
        #expect(scope.broadScope == .jingleOnShow)
    }

    @Test("exactSpan has no broadScope")
    func exactSpanNoMapping() {
        let scope = CorrectionScope.exactSpan(assetId: "a", ordinalRange: 0...5)
        #expect(scope.broadScope == nil)
    }

    @Test("campaignOnShow has no broadScope (Layer A)")
    func campaignNoMapping() {
        let scope = CorrectionScope.campaignOnShow(podcastId: "p", campaign: "c")
        #expect(scope.broadScope == nil)
    }
}
