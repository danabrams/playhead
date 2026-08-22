// SemanticSweepAttributionTests.swift
// playhead-6ruv — the sweep read TALKING ABOUT sponsorship as an ad.
//
// THE FIELD CASE, and it is Dan's own correction of 2026-08-19. Asset
// `CD2976E6`, scan window [1131.6–1210.9], `passA`, `transcriptQuality=good`,
// returning `{"supportLineRefs":[46],"certainty":"strong"}`. The transcript in
// it is Alex Honnold, verbatim from `transcript_chunks`:
//
//   *"I mean, the 1st my 1st couple years, my sponsored through the Northface
//    was like, I think my 1st year was like 10 K a year … you go from just like
//    making some money from sponsors to like making money from other
//    corporations … then I started doing corporate speaking and stuff like
//    that … they also had to get all this sort of marketing material"*
//
// No advertiser, no product pitch, no call to action, no URL, no promo code,
// and nothing to skip. It is a climber describing HOW HE EARNS A LIVING, and
// the model read commercial VOCABULARY as commercial INTENT. An interview show
// whose guests discuss brand deals is the format of both subscribed shows, so
// this is not an edge case.
//
// WHAT THESE SUITES CAN AND CANNOT DO ABOUT IT, stated first because the
// honest answer is the interesting one:
//
//   * They do NOT move Dan's mark. It has no refinement row under it at all,
//     so it is `.unrefined` before this bead and `.unrefined` after — an
//     unrefined coarse verdict is presence with no attributable advertiser BY
//     CONSTRUCTION, because `CoarseScreeningSchema` is a disposition plus a
//     support object with exactly two fields. `dansVetoedMarkIsUnrefined`
//     pins that rather than hiding it.
//   * What they DO is stop it being indistinguishable from a mark the model
//     anchored to a brand. The REFINEMENT pass is asked exactly the question
//     this bead wanted answered — a `passB` `containsAd` row's `spansJSON`
//     carries `commercialIntent`, `ownership` and an `anchors` array naming a
//     KIND — and the composer read nothing out of it but the certainty band.
//     Measured on the 2026-08-19 t4 pull: of the 79 persisted sweep marks,
//     **5 are `.refined` with a naming anchor, 10 are `.suppressed`, 64 are
//     `.unrefined`, 0 are `.unreadable`**.
//   * THE CENTRAL HAZARD IS THE PERMISSIVE BYPASS, and it is the reason the
//     10 exist as their own case. `PermissiveAdClassifier.makeAnchorlessSpan`
//     hardcodes `commercialIntent: .paid` and `ownership: .thirdParty`.
//     Measured on the same pull with the gate REMOVED, all 15 marks that carry
//     a refinement read `paid` / `thirdParty` — so 10 of 79 would carry a "paid
//     third-party ad" judgement no model ever made. That is this bead's own
//     defect one layer down, and `permissiveSpanContributesNoDimensions` is
//     the rail on it.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum AttributionFixture {

    static let assetId = "asset-cd2976e6"
    static let transcriptVersion = "807613cf4b0f2898cc1437afe79b480f"

    /// `7DD870DC` [726.48–738.3], verbatim from `semantic_scan_results` on the
    /// 2026-08-19 t4 pull. A url and a brand span, both anchored to line 46.
    static let namedPayload = """
    [{"anchors":[{"certainty":"strong","evidenceRef":4,"kind":"url","lineRef":46,\
    "resolutionSource":"evidenceRef"},{"certainty":"strong","evidenceRef":5,\
    "kind":"brandSpan","lineRef":46,"resolutionSource":"evidenceRef"}],\
    "certainty":"strong","commercialIntent":"paid","firstLineRef":46,\
    "lastLineRef":46,"ownership":"thirdParty","ownershipInferenceWasSuppressed":false}]
    """

    /// `7DD870DC` [4264.2–4293.96], verbatim from the same pull. The
    /// permissive bypass: no anchors at all, and `paid` / `thirdParty` /
    /// `strong` written by the RUNNER.
    static let permissivePayload = """
    [{"anchors":[],"certainty":"strong","commercialIntent":"paid",\
    "firstLineRef":257,"lastLineRef":257,"ownership":"thirdParty",\
    "ownershipInferenceWasSuppressed":true}]
    """

    /// `7DD870DC` [1477.8–1518.3], verbatim. Carries the third naming kind —
    /// a promo code — alongside the url and the brand span.
    static let promoCodePayload = """
    [{"anchors":[{"certainty":"strong","evidenceRef":6,"kind":"url","lineRef":103,\
    "resolutionSource":"evidenceRef"},{"certainty":"strong","evidenceRef":7,\
    "kind":"brandSpan","lineRef":103,"resolutionSource":"evidenceRef"},\
    {"certainty":"strong","evidenceRef":8,"kind":"promoCode","lineRef":103,\
    "resolutionSource":"evidenceRef"}],"certainty":"strong",\
    "commercialIntent":"paid","firstLineRef":103,"lastLineRef":103,\
    "ownership":"thirdParty","ownershipInferenceWasSuppressed":false}]
    """

    /// Dan's own row, verbatim: `CD2976E6` [1131.6–1210.86], `passA`,
    /// `containsAd`, `good`. Two fields, neither of them an advertiser.
    static let dansCoarsePayload = #"{"supportLineRefs":[46],"certainty":"strong"}"#

    /// A refined span carrying ONLY non-naming anchors. A disclosure phrase is
    /// real and does appear in the field — `7DD870DC` [1477.8–1598.76] carries
    /// one — but *"this episode is sponsored"* names nobody.
    static let disclosureOnlyPayload = """
    [{"anchors":[{"certainty":"strong","evidenceRef":9,"kind":"disclosurePhrase",\
    "lineRef":104,"resolutionSource":"evidenceRef"},{"certainty":"moderate",\
    "kind":"ctaPhrase","lineRef":104,"resolutionSource":"lineRef"}],\
    "certainty":"moderate","commercialIntent":"organic","firstLineRef":104,\
    "lastLineRef":104,"ownership":"show","ownershipInferenceWasSuppressed":false}]
    """

    static func row(
        id: String,
        start: Double,
        end: Double,
        atoms: ClosedRange<Int> = 0...1,
        disposition: CoarseDisposition = .containsAd,
        status: SemanticScanStatus = .success,
        scanPass: String = "passA",
        transcriptQuality: TranscriptQuality = .good,
        spansJSON: String
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: atoms.lowerBound,
            windowLastAtomOrdinal: atoms.upperBound,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: transcriptQuality,
            disposition: disposition,
            spansJSON: spansJSON,
            status: status,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeCohortJSON(promptLabel: "6ruv"),
            transcriptVersion: transcriptVersion
        )
    }

    /// A `passB` `containsAd` row over [700, 760] carrying `payload`.
    static func refinement(
        _ payload: String,
        id: String = "scan-refine",
        start: Double = 726.48,
        end: Double = 738.3,
        disposition: CoarseDisposition = .containsAd,
        status: SemanticScanStatus = .success
    ) -> SemanticScanResult {
        row(id: id, start: start, end: end, disposition: disposition,
            status: status, scanPass: "passB", spansJSON: payload)
    }

    static let extent = SemanticSweepMarkComposer.Extent(start: 726.48, end: 738.3)

    static func attribution(
        _ rows: [SemanticScanResult],
        over extent: SemanticSweepMarkComposer.Extent = extent
    ) -> SemanticSweepMarkComposer.Attribution {
        SemanticSweepMarkComposer.attribution(for: extent, in: rows)
    }

    static func refinementDimensions(
        _ attribution: SemanticSweepMarkComposer.Attribution
    ) -> SemanticSweepMarkComposer.Refinement? {
        guard case .refined(let refinement) = attribution else { return nil }
        return refinement
    }
}

// MARK: - 1. What the model named

@Suite("a mark records WHETHER the model named an advertiser (playhead-6ruv)",
       .timeLimit(.minutes(1)))
struct SemanticSweepAttributionTests {

    fileprivate typealias Fx = AttributionFixture

    /// The 5-of-79 case, on the real payload. A url and a brand span, `paid`,
    /// `thirdParty` — the "nameable advertiser" the bead asked for.
    @Test("a refined span the MODEL judged reports its anchors, intent and ownership")
    func refinedSpanReportsItsDimensions() {
        let attribution = Fx.attribution([Fx.refinement(Fx.namedPayload)])

        #expect(attribution.namesAnAdvertiser)
        let refinement = Fx.refinementDimensions(attribution)
        #expect(refinement?.anchorKinds == [.url, .brandSpan])
        #expect(refinement?.commercialIntents == [.paid])
        #expect(refinement?.ownerships == [.thirdParty])
    }

    /// The third naming kind, on the real payload that carries it.
    @Test("a promo code names an advertiser")
    func promoCodeNames() {
        let attribution = Fx.attribution([Fx.refinement(Fx.promoCodePayload)])

        #expect(Fx.refinementDimensions(attribution)?.anchorKinds
            == [.url, .brandSpan, .promoCode])
        #expect(attribution.namesAnAdvertiser)
    }

    /// THE HAZARD THIS BEAD EXISTS NOT TO REBUILD. The runner hardcodes
    /// `paid` / `thirdParty` / `strong` on the permissive bypass and says so in
    /// its own comment; reading them as the model's judgement would say "a paid
    /// third-party ad" about audio no model ever classified. Measured on the
    /// 2026-08-19 pull, removing this gate makes 10 of the 79 marks read
    /// exactly that.
    @Test("a PERMISSIVE span contributes no dimensions at all — the runner wrote them")
    func permissiveSpanContributesNoDimensions() {
        let attribution = Fx.attribution([Fx.refinement(Fx.permissivePayload)])

        #expect(attribution == .suppressed)
        #expect(!attribution.namesAnAdvertiser)
        #expect(Fx.refinementDimensions(attribution) == nil,
                "and in particular does NOT report the hardcoded paid/thirdParty")
    }

    /// A mark can rest on both kinds of span at once. The genuine one speaks
    /// and the permissive one stays silent — not "the row is suppressed" and
    /// not "the dimensions are unioned across both".
    @Test("a genuine span beside a permissive one reports ONLY the genuine one")
    func mixedRowReportsOnlyTheJudgedSpan() {
        let attribution = Fx.attribution([
            Fx.refinement(Fx.namedPayload, id: "scan-a"),
            Fx.refinement(Fx.disclosureOnlyPayload, id: "scan-b"),
            Fx.refinement(Fx.permissivePayload, id: "scan-c"),
        ])

        let refinement = Fx.refinementDimensions(attribution)
        #expect(refinement?.anchorKinds == [.url, .brandSpan, .disclosurePhrase, .ctaPhrase])
        // `organic`/`show` come from the disclosure span, which the model DID
        // judge. The permissive span's `paid`/`thirdParty` are absent — which
        // is only visible because the disclosure span disagrees with them.
        #expect(refinement?.commercialIntents == [.paid, .organic])
        #expect(refinement?.ownerships == [.thirdParty, .show])
    }

    /// A call to action and a disclosure are COMMERCIAL VOCABULARY with no
    /// advertiser in them — the exact thing the model over-reads. They are
    /// reported, because the model did point at them, and they do not make the
    /// mark nameable.
    @Test("a CTA or a disclosure phrase is not a NAME")
    func ctaAndDisclosureDoNotName() {
        let attribution = Fx.attribution([Fx.refinement(Fx.disclosureOnlyPayload)])

        #expect(Fx.refinementDimensions(attribution)?.anchorKinds
            == [.disclosurePhrase, .ctaPhrase])
        #expect(!attribution.namesAnAdvertiser)
    }

    /// A kind this build does not know must cost ONE anchor, never the whole
    /// payload. A typed decode throws on an unknown raw value and the throw
    /// takes every dimension of every span on the row with it.
    @Test("an unrecognised anchor kind does not destroy the rest of the payload")
    func unknownAnchorKindIsOneAnchor() {
        let payload = """
        [{"anchors":[{"certainty":"strong","kind":"quantumBrandGlyph","lineRef":46,\
        "resolutionSource":"evidenceRef"},{"certainty":"strong","kind":"brandSpan",\
        "lineRef":46,"resolutionSource":"evidenceRef"}],"certainty":"strong",\
        "commercialIntent":"paid","firstLineRef":46,"lastLineRef":46,\
        "ownership":"thirdParty","ownershipInferenceWasSuppressed":false}]
        """
        let attribution = Fx.attribution([Fx.refinement(payload)])

        #expect(Fx.refinementDimensions(attribution)?.anchorKinds == [.brandSpan])
        #expect(Fx.refinementDimensions(attribution)?.commercialIntents == [.paid])
        #expect(attribution.namesAnAdvertiser)
    }

    /// Likewise for the two dimension enums: an unknown value is one dimension
    /// this build cannot read, not a licence to invent `.unknown` — which is a
    /// real case the MODEL can emit and would be indistinguishable from it.
    @Test("an unrecognised intent or ownership is reported as ABSENT, never as .unknown")
    func unknownDimensionIsAbsentNotUnknown() {
        let payload = """
        [{"anchors":[{"certainty":"strong","kind":"brandSpan","lineRef":46,\
        "resolutionSource":"evidenceRef"}],"certainty":"strong",\
        "commercialIntent":"barter","firstLineRef":46,"lastLineRef":46,\
        "ownership":"cooperative","ownershipInferenceWasSuppressed":false}]
        """
        let refinement = Fx.refinementDimensions(Fx.attribution([Fx.refinement(payload)]))

        #expect(refinement?.commercialIntents.isEmpty == true)
        #expect(refinement?.ownerships.isEmpty == true)
        #expect(refinement?.anchorKinds == [.brandSpan])
    }
}

// MARK: - 2. The four claims, held apart

@Suite("presence with no attributable advertiser is its own claim (playhead-6ruv)",
       .timeLimit(.minutes(1)))
struct SemanticSweepAttributionAbsenceTests {

    fileprivate typealias Fx = AttributionFixture

    /// **THE BEAD'S OWN MARK, AND THIS IS THE HONEST RESULT.** Dan's
    /// [1131.6–1210.9] has no refinement under it, so a perfect projection
    /// leaves it exactly as empty as it was. What changes is that the emptiness
    /// is now RECORDED as a property of the verdict rather than looking like a
    /// mark whose advertiser nobody got round to writing down.
    @Test("Dan's CD2976E6 [1131.6-1210.9] mark is UNREFINED, before and after")
    func dansVetoedMarkIsUnrefined() {
        let dansRow = Fx.row(
            id: "scan-dan", start: 1_131.6, end: 1_210.86,
            spansJSON: Fx.dansCoarsePayload
        )
        let extent = SemanticSweepMarkComposer.Extent(start: 1_131.6, end: 1_210.86)

        #expect(Fx.attribution([dansRow], over: extent) == .unrefined)
        #expect(!Fx.attribution([dansRow], over: extent).namesAnAdvertiser)
    }

    /// A refinement that AFFIRMED and yielded no spans is a failure of our
    /// records; a coarse verdict nobody refined is a property of the verdict.
    /// Collapsing them would be the shu5 `.absent` / `.unreadable` mistake in a
    /// new place.
    @Test("named-nothing and refined-unreadably are DIFFERENT attributions")
    func unreadableIsNotUnrefined() {
        let empty = Fx.attribution([Fx.refinement("[]")])
        let garbage = Fx.attribution([Fx.refinement("{\"not\":\"an array\"}")])
        let none = Fx.attribution([Fx.row(id: "scan-coarse", start: 726.48, end: 738.3,
                                          spansJSON: Fx.dansCoarsePayload)])

        #expect(empty == .unreadable)
        #expect(garbage == .unreadable)
        #expect(none == .unrefined)
        #expect(empty != none)
    }

    /// A DECLINED pass B means "found no edges", never "there is no ad" — the
    /// same scoping `corroboration(for:in:)` and `clearedSpans(in:)` apply. It
    /// attributes nothing, and it does not make a mark read `.unreadable`
    /// either: nothing affirmed, so nothing is missing.
    @Test("a DECLINED refinement attributes nothing and claims nothing")
    func declinedRefinementAttributesNothing() {
        let declined = Fx.refinement("[]", disposition: .noAds)

        #expect(Fx.attribution([declined]) == .unrefined)
    }

    /// The field sweep really does end `2581–2676 | abstain | cancelled`. A row
    /// that did not look is not a verdict about the audio, whatever its
    /// disposition column happens to hold.
    @Test("an UNEXAMINED refinement attributes nothing")
    func unexaminedRefinementAttributesNothing() {
        let cancelled = Fx.refinement(Fx.namedPayload, status: .cancelled)

        #expect(Fx.attribution([cancelled]) == .unrefined)
    }

    /// A refinement of OTHER audio says nothing about this mark.
    @Test("a refinement that does not overlap the extent attributes nothing")
    func nonOverlappingRefinementAttributesNothing() {
        let elsewhere = Fx.refinement(Fx.namedPayload, start: 3_000, end: 3_100)

        #expect(Fx.attribution([elsewhere]) == .unrefined)
    }

    /// THE LIMIT, PINNED SO IT CANNOT BE FORGOTTEN. A coarse row is never a
    /// source of dimensions — not even one carrying the refined shape.
    /// Attribution can say the model named a brand; it does not certify that
    /// the PRESENCE verdict under a `.unrefined` mark was the model's.
    ///
    /// playhead-iw7q: this comment used to give the REASON as "a permissive
    /// coarse row is byte-identical to a genuine one", which stopped being true
    /// at schema V61 — `SemanticScanResult.verdictProvenance` now records which
    /// path produced a coarse verdict, and the sweep composer's certainty gate
    /// already reads it. The LIMIT stands anyway, for a different reason: this
    /// type reads DIMENSIONS (`commercialIntent` / `ownership` / anchors) and a
    /// `CoarseSupportSchema` has none of them to read, whatever its provenance.
    /// Wiring the new column into `Attribution` is playhead-6ruv's.
    @Test("a COARSE row never attributes, whatever its payload looks like")
    func coarseRowNeverAttributes() {
        let coarseWearingRefinedClothes = Fx.row(
            id: "scan-odd", start: 726.48, end: 738.3, spansJSON: Fx.namedPayload
        )

        #expect(Fx.attribution([coarseWearingRefinedClothes]) == .unrefined)
    }
}

// MARK: - 3. What gets written down

@Suite("the attribution is persisted, canonically, and moves nothing else (playhead-6ruv)",
       .timeLimit(.minutes(1)))
struct SemanticSweepAttributionPersistenceTests {

    fileprivate typealias Fx = AttributionFixture

    /// The exact bytes, for each of the four claims. Pinned rather than
    /// described, because `AdWindowMaterialIdentity.producerRevisionToken`
    /// hashes this column: a rendering that is merely equivalent is a different
    /// suggest-card identity.
    @Test("each attribution renders to one canonical, pinned string")
    func canonicalRendering() {
        #expect(SemanticSweepMarkComposer.evidenceSources(for: .unrefined)
            == #"["semanticSweep","unrefined"]"#)
        #expect(SemanticSweepMarkComposer.evidenceSources(for: .unreadable)
            == #"["semanticSweep","refinementUnreadable"]"#)
        #expect(SemanticSweepMarkComposer.evidenceSources(for: .suppressed)
            == #"["semanticSweep","refinementSuppressed"]"#)
        #expect(
            SemanticSweepMarkComposer.evidenceSources(
                for: Fx.attribution([Fx.refinement(Fx.namedPayload)])
            ) == #"["semanticSweep","refined","anchor:brandSpan","anchor:url","intent:paid","ownership:thirdParty"]"#
        )
    }

    /// A `Set`'s iteration order is seeded per PROCESS, so an unsorted
    /// rendering is byte-stable within one run and not across runs — which
    /// would make the revision token move for a mark nothing about which
    /// changed. Two orders of the same evidence must render identically.
    @Test("the rendering does not depend on the order the spans were read in")
    func renderingIsOrderIndependent() {
        let forwards = Fx.attribution([
            Fx.refinement(Fx.namedPayload, id: "a"),
            Fx.refinement(Fx.disclosureOnlyPayload, id: "b"),
        ])
        let backwards = Fx.attribution([
            Fx.refinement(Fx.disclosureOnlyPayload, id: "b"),
            Fx.refinement(Fx.namedPayload, id: "a"),
        ])

        let expected = #"["semanticSweep","refined","anchor:brandSpan","anchor:ctaPhrase","#
            + #""anchor:disclosurePhrase","anchor:url","intent:organic","intent:paid","#
            + #""ownership:show","ownership:thirdParty"]"#

        #expect(forwards == backwards)
        #expect(SemanticSweepMarkComposer.evidenceSources(for: forwards)
            == SemanticSweepMarkComposer.evidenceSources(for: backwards))
        #expect(SemanticSweepMarkComposer.evidenceSources(for: forwards) == expected)
    }

    /// `AnalysisStore+CrossUserSharing.evidenceSourcesClaimCatalog` tokenizes
    /// this column on non-alphanumerics and treats a bare `catalog` token as a
    /// claim on the device-local catalog channel, which demotes an imported
    /// window's authority. A sweep mark is local-only and never exported, so
    /// this cannot bite today — which is exactly why it would go unnoticed if a
    /// future token ever spelled it.
    @Test("no token this composer can write claims the CATALOG channel")
    func noTokenClaimsCatalog() {
        let every: [SemanticSweepMarkComposer.Attribution] = [
            .unrefined, .unreadable, .suppressed,
            .refined(SemanticSweepMarkComposer.Refinement(
                anchorKinds: [.url, .brandSpan, .promoCode, .ctaPhrase, .disclosurePhrase],
                commercialIntents: Set(CommercialIntent.allCases),
                ownerships: [.thirdParty, .show, .network, .guest, .unknown]
            )),
        ]

        for attribution in every {
            let rendered = SemanticSweepMarkComposer.evidenceSources(for: attribution) ?? ""
            let tokens = rendered.components(
                separatedBy: CharacterSet.alphanumerics.inverted
            )
            #expect(!tokens.contains { $0.caseInsensitiveCompare("catalog") == .orderedSame },
                    "rendered: \(rendered)")
            #expect(!rendered.isEmpty)
        }
    }

    /// **THE INVARIANT THAT KEEPS THIS OUT OF DAN'S TERRITORY.** Whether the
    /// model named an advertiser is a RECORD, not a tier: treating an
    /// unnameable verdict as a weaker object is a policy question about what a
    /// banner may claim and it is his. Every field of the emitted row except
    /// `evidenceSources` must be identical across all four attributions — the
    /// confidence, the `eligibilityGate`, both edge anchors, and the
    /// content-addressed id.
    @Test("attribution moves evidenceSources and NOTHING else")
    func attributionMovesOnlyEvidenceSources() {
        let extent = SemanticSweepMarkComposer.Extent(
            start: 726.48, end: 738.3, confidence: 0.4666
        )
        let every: [SemanticSweepMarkComposer.Attribution] = [
            .unrefined, .unreadable, .suppressed,
            .refined(SemanticSweepMarkComposer.Refinement(
                anchorKinds: [.url, .brandSpan],
                commercialIntents: [.paid],
                ownerships: [.thirdParty]
            )),
        ]
        let marks = every.map {
            SemanticSweepMarkComposer.makeMark(
                extent, attribution: $0, analysisAssetId: Fx.assetId
            )
        }

        guard let first = marks.first else {
            Issue.record("no marks built")
            return
        }
        for mark in marks.dropFirst() {
            #expect(mark.id == first.id)
            #expect(mark.startTime == first.startTime)
            #expect(mark.endTime == first.endTime)
            #expect(mark.confidence == first.confidence)
            #expect(mark.eligibilityGate == first.eligibilityGate)
            #expect(mark.startEdgeAnchor == first.startEdgeAnchor)
            #expect(mark.endEdgeAnchor == first.endEdgeAnchor)
            #expect(mark.decisionState == first.decisionState)
            #expect(mark.boundaryState == first.boundaryState)
            #expect(mark.detectorVersion == first.detectorVersion)
            #expect(mark.metadataSource == first.metadataSource)
        }
        // And the banner copy stays generic: no advertiser is invented from an
        // anchor that carries a KIND and a lineRef and no text at all.
        for mark in marks {
            #expect(mark.advertiser == nil)
            #expect(mark.product == nil)
            #expect(mark.adDescription == nil)
            #expect(mark.evidenceText == nil)
            #expect(mark.metadataConfidence == nil)
        }
        #expect(Set(marks.compactMap(\.evidenceSources)).count == 4,
                "and all four ARE distinguishable at rest")
    }

    /// End to end through `compose`: the projection has to survive the seven
    /// stages, not merely exist as a function.
    @Test("a composed mark carries its own attribution")
    func composedMarkCarriesItsAttribution() {
        let coarse = Fx.row(id: "scan-coarse", start: 700, end: 760,
                            spansJSON: #"{"supportLineRefs":[46],"certainty":"strong"}"#)
        let named = Fx.refinement(Fx.namedPayload)

        let withRefinement = SemanticSweepMarkComposer.compose(
            scanRows: [coarse, named], existingWindows: [], analysisAssetId: Fx.assetId
        )
        let withoutRefinement = SemanticSweepMarkComposer.compose(
            scanRows: [coarse], existingWindows: [], analysisAssetId: Fx.assetId
        )

        #expect(withRefinement.count == 1)
        #expect(withRefinement.first?.evidenceSources
            == #"["semanticSweep","refined","anchor:brandSpan","anchor:url","intent:paid","ownership:thirdParty"]"#)
        #expect(withoutRefinement.count == 1)
        #expect(withoutRefinement.first?.evidenceSources
            == #"["semanticSweep","unrefined"]"#)
        // Both are still mark-only proposals over unanchored edges.
        for mark in withRefinement + withoutRefinement {
            #expect(mark.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
            #expect(mark.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(mark.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        }
    }

    /// A recompose over unchanged rows must be a true no-op, which is what lets
    /// the store's INSERT-OR-REPLACE and the version-scoped reconcile leave the
    /// row the user is looking at alone.
    @Test("composing twice over the same rows is byte-identical")
    func recomposeIsANoOp() {
        let rows = [
            Fx.row(id: "scan-coarse", start: 700, end: 760,
                   spansJSON: #"{"supportLineRefs":[46],"certainty":"strong"}"#),
            Fx.refinement(Fx.namedPayload),
            Fx.refinement(Fx.permissivePayload, id: "scan-perm"),
        ]

        let first = SemanticSweepMarkComposer.compose(
            scanRows: rows, existingWindows: [], analysisAssetId: Fx.assetId
        )
        let second = SemanticSweepMarkComposer.compose(
            scanRows: rows.reversed(), existingWindows: [], analysisAssetId: Fx.assetId
        )

        #expect(first.map(\.id) == second.map(\.id))
        #expect(first.map(\.evidenceSources) == second.map(\.evidenceSources))
    }
}
