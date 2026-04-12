import Foundation
import Testing

@testable import Playhead

@Suite("SpanHypothesisEngine")
struct SpanHypothesisEngineTests {

    @Test("Lexical hits map to the expected anchor events")
    func mapsHitsToAnchorEvents() throws {
        let disclosureHit = makeHit(
            category: .sponsor,
            text: "sponsored by betterhelp today",
            startTime: 10,
            endTime: 11,
            weight: 1.5
        )
        let sponsorHit = makeHit(
            category: .sponsor,
            text: "Squarespace",
            startTime: 12,
            endTime: 13,
            weight: 1.5
        )
        let promoHit = makeHit(
            category: .promoCode,
            text: "use code save10 at betterhelp",
            startTime: 14,
            endTime: 15,
            weight: 1.2
        )
        let strongURLHit = makeHit(
            category: .urlCTA,
            text: "betterhelp dot com slash podcast",
            startTime: 16,
            endTime: 17,
            weight: 0.95
        )
        let weakURLHit = makeHit(
            category: .urlCTA,
            text: "head to betterhelp dot com",
            startTime: 18,
            endTime: 19,
            weight: 0.8
        )
        let purchaseHit = makeHit(
            category: .purchaseLanguage,
            text: "free trial",
            startTime: 20,
            endTime: 21,
            weight: 0.9
        )
        let markerHit = makeHit(
            category: .transitionMarker,
            text: "back to the show",
            startTime: 22,
            endTime: 23,
            weight: 0.3
        )

        let disclosure = try #require(SpanHypothesisEngine.mapToAnchorEvent(disclosureHit))
        #expect(disclosure.anchorType == .disclosure)
        #expect(disclosure.sponsorEntity?.value == "betterhelp")

        let sponsor = try #require(SpanHypothesisEngine.mapToAnchorEvent(sponsorHit))
        #expect(sponsor.anchorType == .sponsorLexicon)
        #expect(sponsor.sponsorEntity?.value == "squarespace")

        let promo = try #require(SpanHypothesisEngine.mapToAnchorEvent(promoHit))
        #expect(promo.anchorType == .promoCode)
        #expect(promo.sponsorEntity?.value == "betterhelp")

        let strongURL = try #require(SpanHypothesisEngine.mapToAnchorEvent(strongURLHit))
        #expect(strongURL.anchorType == .url)
        #expect(strongURL.sponsorEntity == nil)

        #expect(SpanHypothesisEngine.mapToAnchorEvent(weakURLHit) == nil)
        #expect(SpanHypothesisEngine.mapToAnchorEvent(purchaseHit) == nil)

        let marker = try #require(SpanHypothesisEngine.mapToAnchorEvent(markerHit))
        #expect(marker.anchorType == .transitionMarker)
        #expect(marker.sponsorEntity == nil)
    }

    @Test("seeded, accumulating, confirmed, and closed transitions work end to end")
    func lifecycleTransitions() {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"

        let seed = makeHit(
            category: .sponsor,
            text: "sponsored by betterhelp",
            startTime: 10,
            endTime: 11,
            weight: 1.5
        )
        _ = engine.ingest(seed, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].state == .seeded)
        #expect(engine.activeHypotheses[0].polarity == .startAnchored)
        #expect(engine.activeHypotheses[0].startCandidateTime == -5)
        #expect(engine.activeHypotheses[0].endCandidateTime == 101)

        let body = makeHit(
            category: .purchaseLanguage,
            text: "free trial",
            startTime: 14,
            endTime: 15,
            weight: 0.9
        )
        _ = engine.ingest(body, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].state == .accumulating)
        #expect(engine.activeHypotheses[0].bodyEvidence.count == 1)

        let closer = makeHit(
            category: .promoCode,
            text: "use code save10 at betterhelp",
            startTime: 18,
            endTime: 19,
            weight: 1.2
        )
        let closed = engine.ingest(closer, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.isEmpty)
        #expect(engine.closedHypotheses.count == 1)
        #expect(engine.closedHypotheses[0].state == .closed)
        #expect(engine.closedHypotheses[0].closingAnchor?.anchorType == .promoCode)
        #expect(closed.count == 1)
        #expect(closed[0].closingReason == .explicitClose)
        #expect(closed[0].isSkipEligible)
        #expect(closed[0].sponsorEntity?.value == "betterhelp")
        #expect(closed[0].evidenceText.contains("sponsored by betterhelp"))
        #expect(closed[0].evidenceText.contains("use code save10 at betterhelp"))
    }

    @Test("idle gap closes an old hypothesis before a new one is seeded")
    func idleGapClosesOldHypothesis() {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"

        _ = engine.ingest(
            makeHit(category: .sponsor, text: "sponsored by betterhelp", startTime: 0, endTime: 1, weight: 1.5),
            analysisAssetId: analysisAssetId
        )
        let emitted = engine.ingest(
            makeHit(category: .sponsor, text: "sponsored by squarespace", startTime: 30, endTime: 31, weight: 1.5),
            analysisAssetId: analysisAssetId
        )

        #expect(emitted.count == 1)
        #expect(emitted[0].closingReason == .idleGap)
        #expect(!emitted[0].isSkipEligible)
        #expect(engine.closedHypotheses.count == 1)
        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].sponsorEntity?.value == "squarespace")
    }

    @Test("same-sponsor overlaps merge while cross-sponsor overlaps stay separate")
    func mergeBehaviorRespectsSponsorCompatibility() {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"

        _ = engine.ingest(
            makeHit(category: .sponsor, text: "sponsored by betterhelp", startTime: 0, endTime: 1, weight: 1.5),
            analysisAssetId: analysisAssetId
        )
        _ = engine.ingest(
            makeHit(category: .sponsor, text: "betterhelp", startTime: 8, endTime: 9, weight: 1.5),
            analysisAssetId: analysisAssetId
        )

        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].supportingAnchors.count == 1)

        _ = engine.ingest(
            makeHit(category: .sponsor, text: "sponsored by squarespace", startTime: 12, endTime: 13, weight: 1.5),
            analysisAssetId: analysisAssetId
        )

        #expect(engine.activeHypotheses.count == 2)
        #expect(Set(engine.activeHypotheses.compactMap { $0.sponsorEntity?.value }) == Set(["betterhelp", "squarespace"]))
    }

    @Test("evidence decay lowers the score as time advances")
    func evidenceDecayLowersScore() {
        let event = AnchorEvent(
            anchorType: .disclosure,
            matchedText: "sponsored by betterhelp",
            startTime: 10,
            endTime: 11,
            weight: 1.5,
            sponsorEntity: NormalizedSponsor("betterhelp")
        )
        var hypothesis = SpanHypothesis(seedAnchor: event, config: SpanHypothesisConfig.default.config(for: .disclosure))
        hypothesis.absorb(bodyEvidence: BodyEvidenceItem(
            matchedText: "free trial",
            timestamp: 12,
            weight: 0.9,
            category: .purchaseLanguage
        ))

        let earlyScore = hypothesis.score(at: 12)
        let laterScore = hypothesis.score(at: 42)
        #expect(laterScore < earlyScore)
    }

    @Test("timeout closures remain ineligible unless evidence is strong enough")
    func timeoutEligibilityDependsOnEvidenceStrength() {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"

        _ = engine.ingest(
            makeHit(category: .sponsor, text: "sponsored by betterhelp", startTime: 0, endTime: 1, weight: 1.5),
            analysisAssetId: analysisAssetId
        )

        let closed = engine.finish(analysisAssetId: analysisAssetId, at: 40)

        #expect(closed.count == 1)
        #expect(closed[0].closingReason == .timeout)
        #expect(!closed[0].isSkipEligible)
    }
}

private func makeHit(
    category: LexicalPatternCategory,
    text: String,
    startTime: Double,
    endTime: Double,
    weight: Double
) -> LexicalHit {
    LexicalHit(
        category: category,
        matchedText: text,
        startTime: startTime,
        endTime: endTime,
        weight: weight
    )
}
