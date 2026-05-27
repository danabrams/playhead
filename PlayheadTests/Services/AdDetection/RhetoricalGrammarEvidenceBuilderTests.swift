// RhetoricalGrammarEvidenceBuilderTests.swift
// playhead-xsdz.12: Hermetic unit tests for the rhetorical act-sequence
// grammar detector.
//
// These tests are FULLY hermetic (no FM, no audio, no corpus) so they run on
// every Cmd-U / PlayheadFastTests pass. They prove:
//   1. A full ad arc (HOOK + PROBLEM + SOLUTION + OFFER + CTA) clears the
//      >= 3-role gate and emits ONE high-weight `.rhetoricalGrammar` entry.
//   2. The dominant FP modes score ~0 / emit nothing: a SOLUTION-only
//      editorial brand mention, a CTA-only self-promo, an EVIDENCE+SOLUTION
//      product review (2 roles, below the gate).
//   3. Out-of-order roles are penalized vs. the same roles in canonical order.
//   4. The >= 3-role gate is honored exactly.
//   5. Edge cases (empty / short span, no roles, a role repeated many times).
//   6. The flag-OFF path is byte-identical to current main (no entry built),
//      fused through `BackfillEvidenceFusion`.

import Foundation
import Testing
@testable import Playhead

@Suite("RhetoricalGrammarEvidenceBuilder")
struct RhetoricalGrammarEvidenceBuilderTests {

    // MARK: - Helpers

    private let builder = RhetoricalGrammarEvidenceBuilder()

    private func makeSpan(
        assetId: String = "asset-1",
        startTime: Double = 0.0,
        endTime: Double = 60.0
    ) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 1, lastAtomOrdinal: 2),
            assetId: assetId,
            firstAtomOrdinal: 1,
            lastAtomOrdinal: 2,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: []
        )
    }

    // A blatant full-arc host read: every canonical role, in order.
    private let fullAdText = """
    Have you ever felt tired of tossing and turning all night?
    The problem is that most mattresses are just too firm.
    That's why I switched to Casper, the perfect solution for better sleep.
    Studies show 9 out of 10 sleepers report deeper rest, and it's rated number one.
    Right now you can get 20 percent off with a 100 night money-back guarantee.
    Just go to casper.com and use code SLEEP at checkout.
    """

    // MARK: - Positive: full arc fires high

    @Test("Full HOOK+PROBLEM+SOLUTION+EVIDENCE+OFFER+CTA arc emits one strong entry")
    func fullArcFires() {
        let entries = builder.buildEntries(text: fullAdText, for: makeSpan())
        #expect(entries.count == 1)
        #expect(entries.first?.source == .rhetoricalGrammar)
        // A complete, in-order arc should land at or near the cap.
        #expect((entries.first?.weight ?? 0) >= 0.15)
        // Detail records the roles that formed the arc.
        if case .lexical(let cats)? = entries.first?.detail {
            #expect(cats.count >= 3)
        } else {
            Issue.record("expected .lexical(matchedCategories:) detail")
        }
    }

    @Test("Full arc registers all six roles via assess()")
    func fullArcAllRoles() {
        let assessment = builder.assess(text: fullAdText)
        #expect(assessment != nil)
        let roles = Set(assessment?.orderedRoles ?? [])
        #expect(roles.contains(.hook))
        #expect(roles.contains(.problem))
        #expect(roles.contains(.solution))
        #expect(roles.contains(.evidence))
        #expect(roles.contains(.offer))
        #expect(roles.contains(.cta))
        // In-order arc → perfect canonical consistency.
        #expect((assessment?.orderConsistency ?? 0) == 1.0)
    }

    // MARK: - Negative: dominant FP modes score ~0 / no entry

    @Test("SOLUTION-only editorial brand mention does not fire (1 role)")
    func solutionOnlyEditorial() {
        // A host casually recommending a tool, no problem/offer/CTA framing.
        let text = "Honestly my favorite app for taking notes is Obsidian, I use it every day."
        let entries = builder.buildEntries(text: text, for: makeSpan())
        #expect(entries.isEmpty)
        #expect(builder.assess(text: text) == nil)
    }

    @Test("CTA-only self-promo does not fire (1 role)")
    func ctaOnlySelfPromo() {
        // Classic end-of-episode self-promo: a CTA with no other roles.
        let text = "Thanks for listening! Go to our website and sign up now for the newsletter."
        let assessment = builder.assess(text: text)
        // At most the CTA role is present — below the >= 3 gate.
        #expect((assessment?.orderedRoles.count ?? 0) < 3)
        #expect(builder.buildEntries(text: text, for: makeSpan()).isEmpty)
    }

    @Test("EVIDENCE+SOLUTION product review does not fire (2 roles, below gate)")
    func evidenceSolutionReviewBelowGate() {
        // A reviewer citing proof and recommending — but no OFFER/CTA/HOOK.
        let text = "Studies show this blender is durable. It's the best tool for smoothies."
        let assessment = builder.assess(text: text)
        #expect((assessment?.orderedRoles.count ?? 0) < 3)
        #expect(builder.buildEntries(text: text, for: makeSpan()).isEmpty)
    }

    // MARK: - Order penalty

    @Test("Out-of-order roles score lower than the same roles in canonical order")
    func outOfOrderPenalized() {
        // Same three roles, canonical order: SOLUTION then OFFER then CTA.
        let inOrder = """
        That's why I use Athletic Greens.
        Get 20 percent off your first order.
        Go to athleticgreens.com to sign up now.
        """
        // Same three roles, inverted: CTA, then OFFER, then SOLUTION.
        let reversed = """
        Go to athleticgreens.com to sign up now.
        Get 20 percent off your first order.
        That's why I use Athletic Greens.
        """
        let inOrderAssessment = builder.assess(text: inOrder)
        let reversedAssessment = builder.assess(text: reversed)
        #expect(inOrderAssessment != nil)
        #expect(reversedAssessment != nil)
        // Same role set, so the count component is identical; the only
        // difference is order consistency, so in-order MUST outweigh reversed.
        #expect(
            (inOrderAssessment?.weight ?? 0) > (reversedAssessment?.weight ?? 0)
        )
        #expect((inOrderAssessment?.orderConsistency ?? 0) > (reversedAssessment?.orderConsistency ?? 1))
    }

    @Test("orderConsistency is 1.0 for canonical and 0.0 for fully reversed")
    func orderConsistencyExtremes() {
        let canonical: [RhetoricalRole] = [.hook, .problem, .solution]
        let reversed: [RhetoricalRole] = [.cta, .offer, .solution]
        #expect(RhetoricalGrammarEvidenceBuilder.orderConsistency(of: canonical) == 1.0)
        #expect(RhetoricalGrammarEvidenceBuilder.orderConsistency(of: reversed) == 0.0)
    }

    // MARK: - The >= 3-role gate

    @Test("Exactly 3 distinct roles clears the gate; exactly 2 does not")
    func threeRoleGate() {
        // Three roles: PROBLEM + SOLUTION + CTA.
        let threeRoles = """
        I always struggle with my back.
        That's why I use this chair.
        Go to example.com to learn more.
        """
        // Two of those roles only: SOLUTION + CTA.
        let twoRoles = """
        That's why I use this chair.
        Go to example.com to learn more.
        """
        #expect(builder.assess(text: threeRoles)?.orderedRoles.count == 3)
        #expect((builder.assess(text: twoRoles)?.orderedRoles.count ?? 0) < 3)
        #expect(!builder.buildEntries(text: threeRoles, for: makeSpan()).isEmpty)
        #expect(builder.buildEntries(text: twoRoles, for: makeSpan()).isEmpty)
    }

    // MARK: - Edge cases

    @Test("Empty text yields no entry")
    func emptyText() {
        #expect(builder.buildEntries(text: "", for: makeSpan()).isEmpty)
        #expect(builder.assess(text: "") == nil)
    }

    @Test("Whitespace-only text yields no entry")
    func whitespaceOnly() {
        #expect(builder.assess(text: "   \n  \t ") == nil)
    }

    @Test("Ordinary narration with no role cues yields no entry")
    func noRoles() {
        let text = "We talked about the weather and then the game went into overtime."
        #expect(builder.assess(text: text) == nil)
        #expect(builder.buildEntries(text: text, for: makeSpan()).isEmpty)
    }

    @Test("One role repeated many times still counts as a single distinct role")
    func roleRepeatedManyTimes() {
        // The same CTA cue repeated — distinct-role count must stay 1, never
        // inflating toward the gate. Use a gate-of-1 builder to expose the
        // distinct-role count directly (the default gate of 3 would return nil
        // here, which is itself correct and asserted on the next line).
        let text = """
        Go to example.com.
        Go to example.com.
        Go to example.com.
        Go to example.com.
        """
        let gateOne = RhetoricalGrammarEvidenceBuilder(
            config: .init(minDistinctRoles: 1)
        )
        #expect(gateOne.assess(text: text)?.orderedRoles.count == 1)
        // With the default >= 3 gate a single repeated role is below the gate,
        // so no entry is emitted.
        #expect(builder.buildEntries(text: text, for: makeSpan()).isEmpty)
        #expect(builder.assess(text: text) == nil)
    }

    // MARK: - Weight is bounded by the cap

    @Test("Emitted weight never exceeds the configured maxWeight")
    func weightBounded() {
        let weight = builder.buildEntries(text: fullAdText, for: makeSpan()).first?.weight ?? 0
        #expect(weight <= RhetoricalGrammarEvidenceBuilder.Config.default.maxWeight + 1e-9)
    }

    // MARK: - Fusion integration: flag-OFF identity + capped contribution

    /// Build a ledger via the same fusion path the service uses, with the
    /// grammar entries either present or absent, and return the summed weight.
    private func fusedWeight(rhetoricalEntries: [EvidenceLedgerEntry]) -> Double {
        let fusion = BackfillEvidenceFusion(
            span: makeSpan(),
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            rhetoricalGrammarEntries: rhetoricalEntries,
            mode: .full,
            config: FusionWeightConfig()
        )
        return fusion.buildLedger().reduce(0.0) { $0 + $1.weight }
    }

    @Test("Flag-OFF identity: no grammar entries ⇒ ledger has no .rhetoricalGrammar mass")
    func flagOffIdentity() {
        // With no grammar entries (the flag-off path passes []), the fused
        // ledger contains zero .rhetoricalGrammar contribution — byte-identical
        // to pre-xsdz.12.
        let fusion = BackfillEvidenceFusion(
            span: makeSpan(),
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            rhetoricalGrammarEntries: [],
            mode: .full,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        #expect(!ledger.contains { $0.source == .rhetoricalGrammar })
    }

    @Test("Grammar entry contributes capped mass when present")
    func cappedContributionWhenPresent() {
        let entries = builder.buildEntries(text: fullAdText, for: makeSpan())
        #expect(!entries.isEmpty)
        let withEntry = fusedWeight(rhetoricalEntries: entries)
        let withoutEntry = fusedWeight(rhetoricalEntries: [])
        // Adding a firing grammar entry adds positive but bounded mass.
        #expect(withEntry > withoutEntry)
        #expect(withEntry - withoutEntry <= FusionWeightConfig().rhetoricalGrammarCap + 1e-9)
    }

    @Test("Entry weight is clamped to rhetoricalGrammarCap inside fusion")
    func fusionClampsToCap() {
        // Hand an over-cap entry to fusion; it must be clamped.
        let overCap = EvidenceLedgerEntry(
            source: .rhetoricalGrammar,
            weight: 0.9,
            detail: .lexical(matchedCategories: ["hook", "solution", "cta"])
        )
        let fusion = BackfillEvidenceFusion(
            span: makeSpan(),
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            rhetoricalGrammarEntries: [overCap],
            mode: .full,
            config: FusionWeightConfig()
        )
        let entry = fusion.buildLedger().first { $0.source == .rhetoricalGrammar }
        #expect(entry != nil)
        #expect((entry?.weight ?? 1.0) <= FusionWeightConfig().rhetoricalGrammarCap + 1e-9)
    }

    // MARK: - .lexical-detail reuse must not be misread as a strong anchor

    /// `.rhetoricalGrammar` entries reuse the `.lexical(matchedCategories:)`
    /// detail variant to record their role labels. `FMSuppressionGuard`
    /// inspects that detail (NOT the source) for STRONG lexical anchors
    /// (`sponsor` / `promoCode` / `urlCTA`), so a role label that collided with
    /// one of those raw values would silently re-classify a soft grammar
    /// corroborator as a strong anchor — and wrongly block legitimate FM noAds
    /// suppression. Pin the invariant so a future role rename can't regress it.
    @Test("No rhetorical role label collides with a strong LexicalPatternCategory")
    func roleLabelsDoNotCollideWithStrongLexicalCategories() {
        let strongLexicalRawValues: Set<String> = [
            LexicalPatternCategory.sponsor.rawValue,
            LexicalPatternCategory.promoCode.rawValue,
            LexicalPatternCategory.urlCTA.rawValue,
        ]
        for role in RhetoricalRole.allCases {
            #expect(
                !strongLexicalRawValues.contains(role.label),
                "role label '\(role.label)' collides with a strong lexical category"
            )
        }
    }

    /// End-to-end through the real `FMSuppressionGuard`: a firing grammar entry
    /// (its `.lexical` detail carries role labels including "cta") must NOT
    /// register as a strong anchor, so an otherwise-clean noAds consensus still
    /// triggers suppression. If the role labels ever collided with a strong
    /// lexical category, `hasStrongAnchors` would flip and this would fail.
    @Test("A firing grammar entry does not block FM noAds suppression as a strong anchor")
    func grammarEntryIsNotAStrongAnchorInSuppressionGuard() {
        let grammarEntries = builder.buildEntries(text: fullAdText, for: makeSpan())
        #expect(!grammarEntries.isEmpty)
        let guardWithGrammar = FMSuppressionGuard(
            overlappingFMResults: [
                FMSuppressionWindow(disposition: .noAds, band: .moderate),
                FMSuppressionWindow(disposition: .noAds, band: .moderate),
            ],
            ledger: grammarEntries,
            anchorProvenance: []
        )
        #expect(guardWithGrammar.evaluate().isTriggered)

        // Control: a genuine strong lexical anchor (urlCTA) DOES block, proving
        // the guard is actually exercising the strong-anchor path above.
        let strongAnchorEntry = EvidenceLedgerEntry(
            source: .lexical,
            weight: 0.3,
            detail: .lexical(matchedCategories: [LexicalPatternCategory.urlCTA.rawValue])
        )
        let guardWithAnchor = FMSuppressionGuard(
            overlappingFMResults: [
                FMSuppressionWindow(disposition: .noAds, band: .moderate),
                FMSuppressionWindow(disposition: .noAds, band: .moderate),
            ],
            ledger: [strongAnchorEntry],
            anchorProvenance: []
        )
        #expect(!guardWithAnchor.evaluate().isTriggered)
    }
}
