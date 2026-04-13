import Foundation
import Testing

@testable import Playhead

@Suite("SpanHypothesisEngine")
struct SpanHypothesisEngineTests {

    @Test("Lexical hits map to anchor events using real scanner-shaped matches")
    func mapsHitsToAnchorEvents() throws {
        let sponsorLexiconScanner = makeScanner(podcastSponsorLexicon: "Squarespace")

        let disclosureHit = try scanHit(
            category: .sponsor,
            text: "this episode is sponsored by betterhelp today",
            startTime: 10,
            endTime: 11,
            matching: { $0.matchedText == "sponsored by" }
        )
        let sponsorHit = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "squarespace can build your website",
            startTime: 12,
            endTime: 13,
            matching: { $0.matchedText == "squarespace" }
        )
        let promoHit = try scanHit(
            category: .promoCode,
            text: "use code save10 at checkout",
            startTime: 14,
            endTime: 15,
            matching: { $0.matchedText == "use code save10" }
        )
        let strongURLHit = try scanHit(
            category: .urlCTA,
            text: "visit betterhelp.com/podcast for details",
            startTime: 16,
            endTime: 17,
            matching: { $0.weight == 0.95 }
        )
        let weakURLHit = try scanHit(
            category: .urlCTA,
            text: "head to betterhelp for details",
            startTime: 18,
            endTime: 19,
            matching: { $0.weight < 0.95 }
        )
        let purchaseHit = try scanHit(
            category: .purchaseLanguage,
            text: "free trial for new members",
            startTime: 20,
            endTime: 21
        )
        let markerHit = try scanHit(
            category: .transitionMarker,
            text: "back to the show",
            startTime: 22,
            endTime: 23
        )

        #expect(disclosureHit.matchedText == "sponsored by")
        let disclosure = try #require(SpanHypothesisEngine.mapToAnchorEvent(disclosureHit))
        #expect(disclosure.anchorType == .disclosure)
        #expect(disclosure.sponsorEntity == nil)

        #expect(sponsorHit.matchedText == "squarespace")
        let sponsor = try #require(SpanHypothesisEngine.mapToAnchorEvent(sponsorHit))
        #expect(sponsor.anchorType == .sponsorLexicon)
        #expect(sponsor.sponsorEntity?.value == "squarespace")

        #expect(promoHit.matchedText == "use code save10")
        let promo = try #require(SpanHypothesisEngine.mapToAnchorEvent(promoHit))
        #expect(promo.anchorType == .promoCode)
        #expect(promo.sponsorEntity == nil)

        #expect(strongURLHit.matchedText == "betterhelp.com")
        let strongURL = try #require(SpanHypothesisEngine.mapToAnchorEvent(strongURLHit))
        #expect(strongURL.anchorType == .url)
        #expect(strongURL.sponsorEntity == nil)

        #expect(SpanHypothesisEngine.mapToAnchorEvent(weakURLHit) == nil)
        #expect(SpanHypothesisEngine.mapToBodyEvidence(weakURLHit) != nil)
        #expect(SpanHypothesisEngine.mapToAnchorEvent(purchaseHit) == nil)

        let marker = try #require(SpanHypothesisEngine.mapToAnchorEvent(markerHit))
        #expect(marker.anchorType == .transitionMarker)
        #expect(marker.sponsorEntity == nil)
    }

    @Test("scanner-shaped disclosure and promo hits still close the active hypothesis")
    func lifecycleTransitions() throws {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"

        let seed = try scanHit(
            category: .sponsor,
            text: "this episode is sponsored by betterhelp",
            startTime: 10,
            endTime: 11,
            matching: { $0.matchedText == "sponsored by" }
        )
        _ = engine.ingest(seed, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].state == .seeded)
        #expect(engine.activeHypotheses[0].polarity == .startAnchored)
        #expect(engine.activeHypotheses[0].sponsorEntity == nil)

        let body = try scanHit(
            category: .purchaseLanguage,
            text: "free trial for new members",
            startTime: 14,
            endTime: 15
        )
        _ = engine.ingest(body, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].state == .accumulating)
        #expect(engine.activeHypotheses[0].bodyEvidence.count == 1)

        let closer = try scanHit(
            category: .promoCode,
            text: "use code save10 at checkout",
            startTime: 18,
            endTime: 19,
            matching: { $0.matchedText == "use code save10" }
        )
        let closed = engine.ingest(closer, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.isEmpty)
        #expect(engine.closedHypotheses.count == 1)
        #expect(engine.closedHypotheses[0].state == .closed)
        #expect(engine.closedHypotheses[0].closingAnchor?.anchorType == .promoCode)
        #expect(closed.count == 1)
        #expect(closed[0].closingReason == .explicitClose)
        #expect(closed[0].isSkipEligible)
        #expect(closed[0].sponsorEntity == nil)
        #expect(closed[0].evidenceText.contains("sponsored by"))
        #expect(closed[0].evidenceText.contains("use code save10"))
    }

    @Test("idle gap closes an old hypothesis before a new one is seeded")
    func idleGapClosesOldHypothesis() throws {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"
        let sponsorLexiconScanner = makeScanner(podcastSponsorLexicon: "Squarespace")

        let firstHit = try scanHit(
            category: .sponsor,
            text: "this episode is sponsored by betterhelp",
            startTime: 0,
            endTime: 1,
            matching: { $0.matchedText == "sponsored by" }
        )
        _ = engine.ingest(firstHit, analysisAssetId: analysisAssetId)

        let secondHit = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "squarespace makes websites easy",
            startTime: 30,
            endTime: 31,
            matching: { $0.matchedText == "squarespace" }
        )
        let emitted = engine.ingest(secondHit, analysisAssetId: analysisAssetId)

        #expect(emitted.count == 1)
        #expect(emitted[0].closingReason == .idleGap)
        #expect(!emitted[0].isSkipEligible)
        #expect(engine.closedHypotheses.count == 1)
        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].sponsorEntity?.value == "squarespace")
    }

    @Test("same-sponsor overlaps merge while cross-sponsor overlaps stay separate")
    func mergeBehaviorRespectsSponsorCompatibility() throws {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"
        let sponsorLexiconScanner = makeScanner(podcastSponsorLexicon: "BetterHelp, Squarespace")

        let seed = try scanHit(
            category: .sponsor,
            text: "this episode is sponsored by betterhelp",
            startTime: 0,
            endTime: 1,
            matching: { $0.matchedText == "sponsored by" }
        )
        _ = engine.ingest(seed, analysisAssetId: analysisAssetId)

        let sameSponsor = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "betterhelp can help you find a therapist",
            startTime: 8,
            endTime: 9,
            matching: { $0.matchedText == "betterhelp" }
        )
        _ = engine.ingest(sameSponsor, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].supportingAnchors.count == 1)
        #expect(engine.activeHypotheses[0].sponsorEntity?.value == "betterhelp")

        let otherSponsor = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "squarespace helps you publish online",
            startTime: 12,
            endTime: 13,
            matching: { $0.matchedText == "squarespace" }
        )
        _ = engine.ingest(otherSponsor, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.count == 2)
        #expect(Set(engine.activeHypotheses.compactMap { $0.sponsorEntity?.value }) == Set(["betterhelp", "squarespace"]))
    }

    @Test("sponsor-less promo closes prefer the stronger compatible owner over a newer weaker span")
    func sponsorlessPromoCloseAnchorsChooseStrongerCompatibleSpan() throws {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"
        let sponsorLexiconScanner = makeScanner(podcastSponsorLexicon: "BetterHelp, Squarespace")

        let betterHelpDisclosure = try scanHit(
            category: .sponsor,
            text: "this episode is sponsored by betterhelp",
            startTime: 0,
            endTime: 1,
            matching: { $0.matchedText == "sponsored by" }
        )
        _ = engine.ingest(betterHelpDisclosure, analysisAssetId: analysisAssetId)

        let betterHelpLexicon = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "betterhelp can help you find a therapist",
            startTime: 2,
            endTime: 3,
            matching: { $0.matchedText == "betterhelp" }
        )
        _ = engine.ingest(betterHelpLexicon, analysisAssetId: analysisAssetId)

        let squarespaceLexicon = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "squarespace helps you publish online",
            startTime: 8,
            endTime: 9,
            matching: { $0.matchedText == "squarespace" }
        )
        _ = engine.ingest(squarespaceLexicon, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.count == 2)
        #expect(engine.activeHypotheses.map(\.sponsorEntity?.value) == ["betterhelp", "squarespace"])

        let sponsorlessPromoClose = try scanHit(
            category: .promoCode,
            text: "use code save10 at checkout",
            startTime: 12,
            endTime: 13,
            matching: { $0.matchedText == "use code save10" }
        )
        let closed = engine.ingest(sponsorlessPromoClose, analysisAssetId: analysisAssetId)

        #expect(closed.count == 1)
        #expect(closed[0].sponsorEntity?.value == "betterhelp")
        #expect(engine.closedHypotheses.count == 1)
        #expect(engine.closedHypotheses[0].sponsorEntity?.value == "betterhelp")
        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].sponsorEntity?.value == "squarespace")
    }

    @Test("sponsor-less return markers prefer the stronger current hypothesis over an older stale one")
    func sponsorlessReturnMarkersChooseStrongerCurrentSpan() throws {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"
        let sponsorLexiconScanner = makeScanner(podcastSponsorLexicon: "BetterHelp, Squarespace")

        let betterHelpDisclosure = try scanHit(
            category: .sponsor,
            text: "this episode is sponsored by betterhelp",
            startTime: 0,
            endTime: 1,
            matching: { $0.matchedText == "sponsored by" }
        )
        _ = engine.ingest(betterHelpDisclosure, analysisAssetId: analysisAssetId)

        let betterHelpLexicon = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "betterhelp can help you find a therapist",
            startTime: 2,
            endTime: 3,
            matching: { $0.matchedText == "betterhelp" }
        )
        _ = engine.ingest(betterHelpLexicon, analysisAssetId: analysisAssetId)

        let squarespaceSeed = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "squarespace makes publishing easy",
            startTime: 10,
            endTime: 11,
            matching: { $0.matchedText == "squarespace" }
        )
        _ = engine.ingest(squarespaceSeed, analysisAssetId: analysisAssetId)

        let squarespaceSupport = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "squarespace lets you build a website fast",
            startTime: 14,
            endTime: 15,
            matching: { $0.matchedText == "squarespace" }
        )
        _ = engine.ingest(squarespaceSupport, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.count == 2)
        #expect(engine.activeHypotheses.map(\.sponsorEntity?.value) == ["betterhelp", "squarespace"])

        let returnMarker = try scanHit(
            category: .transitionMarker,
            text: "back to the show",
            startTime: 18,
            endTime: 19
        )
        let closed = engine.ingest(returnMarker, analysisAssetId: analysisAssetId)

        #expect(closed.count == 1)
        #expect(closed[0].closingReason == .returnMarker)
        #expect(closed[0].sponsorEntity?.value == "squarespace")
        #expect(engine.closedHypotheses.count == 1)
        #expect(engine.closedHypotheses[0].sponsorEntity?.value == "squarespace")
        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].sponsorEntity?.value == "betterhelp")
    }

    @Test("sponsor-less return markers do not let stale high-score hypotheses steal the closer")
    func sponsorlessReturnMarkersDoNotPreferStaleHighScoreHypothesis() throws {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"
        let sponsorLexiconScanner = makeScanner(podcastSponsorLexicon: "BetterHelp, Squarespace")

        let betterHelpDisclosure = try scanHit(
            category: .sponsor,
            text: "this episode is sponsored by betterhelp",
            startTime: 0,
            endTime: 1,
            matching: { $0.matchedText == "sponsored by" }
        )
        _ = engine.ingest(betterHelpDisclosure, analysisAssetId: analysisAssetId)

        let betterHelpLexicon = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "betterhelp can help you find a therapist",
            startTime: 2,
            endTime: 3,
            matching: { $0.matchedText == "betterhelp" }
        )
        _ = engine.ingest(betterHelpLexicon, analysisAssetId: analysisAssetId)

        for time in [4.0, 5.0, 6.0, 7.0] {
            let purchaseHit = try scanHit(
                category: .purchaseLanguage,
                text: "free trial for new members",
                startTime: time,
                endTime: time + 0.5
            )
            _ = engine.ingest(purchaseHit, analysisAssetId: analysisAssetId)
        }

        let squarespaceSeed = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "squarespace makes publishing easy",
            startTime: 15,
            endTime: 16,
            matching: { $0.matchedText == "squarespace" }
        )
        _ = engine.ingest(squarespaceSeed, analysisAssetId: analysisAssetId)

        let squarespaceSupport = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "squarespace lets you build a website fast",
            startTime: 17,
            endTime: 18,
            matching: { $0.matchedText == "squarespace" }
        )
        _ = engine.ingest(squarespaceSupport, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.count == 2)

        let returnMarker = try scanHit(
            category: .transitionMarker,
            text: "back to the show",
            startTime: 19,
            endTime: 20
        )
        let closed = engine.ingest(returnMarker, analysisAssetId: analysisAssetId)

        #expect(closed.count == 1)
        #expect(closed[0].sponsorEntity?.value == "squarespace")
        #expect(engine.closedHypotheses.count == 1)
        #expect(engine.closedHypotheses[0].sponsorEntity?.value == "squarespace")
        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].sponsorEntity?.value == "betterhelp")
    }

    @Test("sponsor-less promo closes do not let stale high-score hypotheses steal the closer")
    func sponsorlessPromoClosesDoNotPreferStaleHighScoreHypothesis() throws {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"
        let sponsorLexiconScanner = makeScanner(podcastSponsorLexicon: "BetterHelp, Squarespace")

        let betterHelpDisclosure = try scanHit(
            category: .sponsor,
            text: "this episode is sponsored by betterhelp",
            startTime: 0,
            endTime: 1,
            matching: { $0.matchedText == "sponsored by" }
        )
        _ = engine.ingest(betterHelpDisclosure, analysisAssetId: analysisAssetId)

        let betterHelpLexicon = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "betterhelp can help you find a therapist",
            startTime: 2,
            endTime: 3,
            matching: { $0.matchedText == "betterhelp" }
        )
        _ = engine.ingest(betterHelpLexicon, analysisAssetId: analysisAssetId)

        for time in [4.0, 5.0, 6.0, 7.0] {
            let purchaseHit = try scanHit(
                category: .purchaseLanguage,
                text: "free trial for new members",
                startTime: time,
                endTime: time + 0.5
            )
            _ = engine.ingest(purchaseHit, analysisAssetId: analysisAssetId)
        }

        let squarespaceSeed = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "squarespace makes publishing easy",
            startTime: 15,
            endTime: 16,
            matching: { $0.matchedText == "squarespace" }
        )
        _ = engine.ingest(squarespaceSeed, analysisAssetId: analysisAssetId)

        let squarespaceSupport = try scanHit(
            scanner: sponsorLexiconScanner,
            category: .sponsor,
            text: "squarespace lets you build a website fast",
            startTime: 17,
            endTime: 18,
            matching: { $0.matchedText == "squarespace" }
        )
        _ = engine.ingest(squarespaceSupport, analysisAssetId: analysisAssetId)

        #expect(engine.activeHypotheses.count == 2)

        let promoClose = try scanHit(
            category: .promoCode,
            text: "use code buildit at checkout",
            startTime: 19,
            endTime: 20,
            matching: { $0.matchedText == "use code buildit" }
        )
        let closed = engine.ingest(promoClose, analysisAssetId: analysisAssetId)

        #expect(closed.count == 1)
        #expect(closed[0].sponsorEntity?.value == "squarespace")
        #expect(engine.closedHypotheses.count == 1)
        #expect(engine.closedHypotheses[0].sponsorEntity?.value == "squarespace")
        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].sponsorEntity?.value == "betterhelp")
    }

    @Test("orphan promo anchors seed hypotheses but do not emit skip-eligible spans")
    func orphanPromoCloseAnchorDoesNotEmit() throws {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"

        let promoHit = try scanHit(
            category: .promoCode,
            text: "use code save10 at checkout",
            startTime: 10,
            endTime: 11,
            matching: { $0.matchedText == "use code save10" }
        )
        let emitted = engine.ingest(promoHit, analysisAssetId: analysisAssetId)

        #expect(emitted.isEmpty)
        #expect(engine.closedHypotheses.isEmpty)
        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].anchorType == .promoCode)

        let closed = engine.finish(analysisAssetId: analysisAssetId, at: 40)
        #expect(closed.count == 1)
        #expect(closed[0].closingReason == .timeout)
        #expect(!closed[0].isSkipEligible)
    }

    @Test("orphan url anchors seed hypotheses but do not emit skip-eligible spans")
    func orphanURLCloseAnchorDoesNotEmit() throws {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"

        let urlHit = try scanHit(
            category: .urlCTA,
            text: "visit betterhelp.com/podcast for details",
            startTime: 10,
            endTime: 11,
            matching: { $0.weight == 0.95 }
        )
        let emitted = engine.ingest(urlHit, analysisAssetId: analysisAssetId)

        #expect(emitted.isEmpty)
        #expect(engine.closedHypotheses.isEmpty)
        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].anchorType == .url)

        let closed = engine.finish(analysisAssetId: analysisAssetId, at: 40)
        #expect(closed.count == 1)
        #expect(closed[0].closingReason == .timeout)
        #expect(!closed[0].isSkipEligible)
    }

    @Test("confirmed CTA-only end anchored hypotheses expand boundaries at close time without mutating their live window")
    func confirmedCTAOnlyEndAnchoredHypothesesExpandAtClose() throws {
        let urlConfig = SpanHypothesisConfig.defaultURLConfig
        var engine = SpanHypothesisEngine(
            config: SpanHypothesisConfig(
                minConfirmedEvidence: 0.95
            ),
            boundaryExpansionContext: .init(
                featureWindows: [
                    makeFeatureWindow(start: 15, end: 16, pauseProb: 0.95, rms: 0.01),
                    makeFeatureWindow(start: 110, end: 111, pauseProb: 0.96, rms: 0.01),
                ],
                transcriptChunks: []
            )
        )
        let analysisAssetId = "asset-span-engine"

        let urlHit = try scanHit(
            category: .urlCTA,
            text: "visit betterhelp.com/podcast for details",
            startTime: 100,
            endTime: 101,
            matching: { $0.weight == 0.95 }
        )
        let emitted = engine.ingest(urlHit, analysisAssetId: analysisAssetId)

        #expect(emitted.isEmpty)
        #expect(engine.activeHypotheses.count == 1)
        #expect(engine.activeHypotheses[0].state == .confirmed)
        let expectedStartCandidate = urlHit.startTime - urlConfig.backwardSearchRadius
        let expectedEndCandidate = urlHit.endTime + urlConfig.forwardSearchRadius
        #expect(abs(engine.activeHypotheses[0].startCandidateTime - expectedStartCandidate) < 0.001)
        #expect(abs(engine.activeHypotheses[0].endCandidateTime - expectedEndCandidate) < 0.001)
        #expect(engine.activeHypotheses[0].expandedBoundary == nil)

        let closed = engine.finish(analysisAssetId: analysisAssetId, at: urlHit.endTime)

        #expect(closed.count == 1)
        #expect(closed[0].startTime == 15.0)
        #expect(closed[0].endTime == 111.0)
        #expect(closed[0].isSkipEligible)

        #expect(engine.closedHypotheses.count == 1)
        #expect(abs(engine.closedHypotheses[0].startCandidateTime - expectedStartCandidate) < 0.001)
        #expect(abs(engine.closedHypotheses[0].endCandidateTime - expectedEndCandidate) < 0.001)
        #expect(engine.closedHypotheses[0].expandedBoundary?.startTime == 15.0)
        #expect(engine.closedHypotheses[0].expandedBoundary?.endTime == 111.0)
        #expect(engine.closedHypotheses[0].expandedBoundary?.source == .acousticOnly)
    }

    @Test("evidence decay lowers the score as time advances")
    func evidenceDecayLowersScore() {
        let event = AnchorEvent(
            anchorType: .disclosure,
            matchedText: "sponsored by",
            startTime: 10,
            endTime: 11,
            weight: 1.0,
            sponsorEntity: nil
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
    func timeoutEligibilityDependsOnEvidenceStrength() throws {
        var engine = SpanHypothesisEngine()
        let analysisAssetId = "asset-span-engine"

        let hit = try scanHit(
            category: .sponsor,
            text: "this episode is sponsored by betterhelp",
            startTime: 0,
            endTime: 1,
            matching: { $0.matchedText == "sponsored by" }
        )
        _ = engine.ingest(hit, analysisAssetId: analysisAssetId)

        let closed = engine.finish(analysisAssetId: analysisAssetId, at: 40)

        #expect(closed.count == 1)
        #expect(closed[0].closingReason == .timeout)
        #expect(!closed[0].isSkipEligible)
    }
}

private enum SpanHypothesisEngineTestError: Error {
    case missingHit(category: LexicalPatternCategory, text: String)
}

private func makeScanner(podcastSponsorLexicon: String? = nil) -> LexicalScanner {
    guard let podcastSponsorLexicon else { return LexicalScanner() }

    return LexicalScanner(
        podcastProfile: PodcastProfile(
            podcastId: "podcast-id",
            sponsorLexicon: podcastSponsorLexicon,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0,
            observationCount: 0,
            mode: "test",
            recentFalseSkipSignals: 0
        )
    )
}

private func scanHit(
    scanner: LexicalScanner = LexicalScanner(),
    category: LexicalPatternCategory,
    text: String,
    startTime: Double,
    endTime: Double,
    matching predicate: ((LexicalHit) -> Bool)? = nil
) throws -> LexicalHit {
    let hits = scanner.scanChunk(makeChunk(text: text, startTime: startTime, endTime: endTime))
        .filter { hit in
            hit.category == category && (predicate?(hit) ?? true)
        }

    guard let hit = hits.first else {
        throw SpanHypothesisEngineTestError.missingHit(category: category, text: text)
    }
    return hit
}

private func makeChunk(text: String, startTime: Double, endTime: Double) -> TranscriptChunk {
    TranscriptChunk(
        id: UUID().uuidString,
        analysisAssetId: "analysis-asset",
        segmentFingerprint: UUID().uuidString,
        chunkIndex: 0,
        startTime: startTime,
        endTime: endTime,
        text: text,
        normalizedText: TranscriptEngineService.normalizeText(text),
        pass: "final",
        modelVersion: "test",
        transcriptVersion: nil,
        atomOrdinal: nil
    )
}

private func makeFeatureWindow(
    start: Double,
    end: Double,
    pauseProb: Double,
    rms: Double
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: "analysis-asset",
        startTime: start,
        endTime: end,
        rms: rms,
        spectralFlux: 0,
        musicProbability: 0,
        pauseProbability: pauseProb,
        speakerClusterId: nil,
        jingleHash: nil,
        featureVersion: 1
    )
}
