// SponsorAdCueGateTests.swift
// playhead-xsdz.69: a sponsor brand is ATTENTION, not a verdict — it promotes to
// a span (→ banner) only with a co-occurring CONFIRMING ad-cue. This kills the
// editorial brand-as-sponsor false positive (sports teams named after their
// title sponsors: "the Red Bull team won", Ineos, Ferrari, Mercedes) while real
// ad reads — which carry a disclosure frame / URL / promo code / FM-positive —
// still fire.
//
// These pin the shared `AdDetectionService.hypothesisHasConfirmingAdCue` helper
// that gates the sponsor promotion.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-xsdz.69 — sponsor requires a confirming ad-cue")
struct SponsorAdCueGateTests {

    private func sponsorSeed() -> AnchorEvent {
        AnchorEvent(
            anchorType: .sponsorLexicon,
            matchedText: "red bull",
            startTime: 100.0,
            endTime: 101.0,
            weight: 1.0,
            sponsorEntity: NormalizedSponsor("Red Bull")
        )
    }

    private func bareSponsorHypothesis() -> SpanHypothesis {
        SpanHypothesis(
            seedAnchor: sponsorSeed(),
            config: SpanHypothesisConfig.defaultSponsorLexiconConfig
        )
    }

    private func cue(_ type: AnchorType, _ text: String) -> AnchorEvent {
        AnchorEvent(
            anchorType: type,
            matchedText: text,
            startTime: 99.0,
            endTime: 100.0,
            weight: 1.0,
            sponsorEntity: nil
        )
    }

    @Test("the activation flag defaults on")
    func flagDefaultsOn() {
        #expect(AdDetectionService.sponsorRequiresConfirmingAdCue == true)
    }

    @Test("a bare brand mention (no ad-cue) is NOT a confirmed ad — the editorial FP")
    func bareBrandHasNoConfirmingCue() {
        // "the Red Bull team won" — sponsor lexicon hit, matched sponsor entity,
        // but no disclosure / URL / promo / FM-positive anywhere.
        var h = bareSponsorHypothesis()
        h.sponsorEntity = NormalizedSponsor("Red Bull")
        #expect(AdDetectionService.hypothesisHasConfirmingAdCue(h) == false,
                "a bare brand mention has no confirming ad-cue and must not promote")
    }

    @Test("a disclosure frame near the brand IS a confirming ad-cue — real ad read fires")
    func disclosureIsConfirming() {
        var h = bareSponsorHypothesis()
        h.supportingAnchors = [cue(.disclosure, "brought to you by")]
        #expect(AdDetectionService.hypothesisHasConfirmingAdCue(h) == true)
    }

    @Test("a URL is a confirming ad-cue")
    func urlIsConfirming() {
        var h = bareSponsorHypothesis()
        h.supportingAnchors = [cue(.url, "redbull.com")]
        #expect(AdDetectionService.hypothesisHasConfirmingAdCue(h) == true)
    }

    @Test("a promo code is a confirming ad-cue")
    func promoCodeIsConfirming() {
        var h = bareSponsorHypothesis()
        h.supportingAnchors = [cue(.promoCode, "code POD20")]
        #expect(AdDetectionService.hypothesisHasConfirmingAdCue(h) == true)
    }

    @Test("an FM-positive is a confirming ad-cue (FM disposition unchanged, out of scope)")
    func fmPositiveIsConfirming() {
        var h = bareSponsorHypothesis()
        h.supportingAnchors = [cue(.fmPositive, "sounds like an ad")]
        #expect(AdDetectionService.hypothesisHasConfirmingAdCue(h) == true)
    }

    @Test("a transition marker alone is NOT a confirming ad-cue")
    func transitionMarkerNotConfirming() {
        var h = bareSponsorHypothesis()
        h.supportingAnchors = [cue(.transitionMarker, "and we're back")]
        #expect(AdDetectionService.hypothesisHasConfirmingAdCue(h) == false)
    }

    @Test("a confirming cue on the CLOSING anchor also counts")
    func closingAnchorCueCounts() {
        var h = bareSponsorHypothesis()
        h.closingAnchor = cue(.url, "redbull.com")
        #expect(AdDetectionService.hypothesisHasConfirmingAdCue(h) == true)
    }
}
