// CompositeFingerprintTests.swift
// playhead-ef2.5.5: Tests for composite fingerprint upgrade — transcript,
// acoustic, and sponsor-marker signatures with composite scoring.

import Foundation
import Testing
@testable import Playhead

// MARK: - TranscriptSignature Tests

@Suite("TranscriptSignature")
struct TranscriptSignatureTests {

    @Test("Identical texts produce similarity of 1.0")
    func identicalTexts() {
        let sig1 = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "This episode is brought to you by BetterHelp",
            transcriptReliability: 1.0
        )!
        let sig2 = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "This episode is brought to you by BetterHelp",
            transcriptReliability: 1.0
        )!
        #expect(sig1.similarity(to: sig2) == 1.0)
    }

    @Test("Similar texts with ASR errors produce high similarity")
    func asrErrorTolerance() {
        let sig1 = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "This episode is brought to you by BetterHelp online therapy",
            transcriptReliability: 1.0
        )!
        // Simulated ASR error: "BetterHelp" -> "better help"
        let sig2 = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "This episode is brought to you by better help online therapy",
            transcriptReliability: 1.0
        )!
        let sim = sig1.similarity(to: sig2)
        #expect(sim > 0.7, "ASR error should still produce high similarity, got \(sim)")
    }

    @Test("Completely different texts produce low similarity")
    func differentTexts() {
        let sig1 = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "This episode is brought to you by BetterHelp online therapy",
            transcriptReliability: 1.0
        )!
        let sig2 = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "The weather forecast for tomorrow shows sunny skies and warm temperatures",
            transcriptReliability: 1.0
        )!
        let sim = sig1.similarity(to: sig2)
        #expect(sim < 0.4, "Different texts should produce low similarity, got \(sim)")
    }

    @Test("3-gram generation produces correct features")
    func threeGramGeneration() {
        let ngrams = CompositeFingerprintBuilder.generate3Grams("abcdef")
        #expect(ngrams.contains("abc"))
        #expect(ngrams.contains("bcd"))
        #expect(ngrams.contains("cde"))
        #expect(ngrams.contains("def"))
        #expect(ngrams.count == 4)
    }

    @Test("3-gram generation handles short text")
    func threeGramShortText() {
        let ngrams1 = CompositeFingerprintBuilder.generate3Grams("ab")
        #expect(ngrams1 == Set(["ab"]))

        let ngrams2 = CompositeFingerprintBuilder.generate3Grams("")
        #expect(ngrams2.isEmpty)

        let ngrams3 = CompositeFingerprintBuilder.generate3Grams("abc")
        #expect(ngrams3 == Set(["abc"]))
    }

    @Test("Builder returns nil for low reliability")
    func lowReliabilityReturnsNil() {
        let sig = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "Some transcript text here",
            transcriptReliability: 0.2
        )
        #expect(sig == nil)
    }

    @Test("Builder returns nil for empty text")
    func emptyTextReturnsNil() {
        let sig = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "",
            transcriptReliability: 1.0
        )
        #expect(sig == nil)
    }

    @Test("MinHash signature has correct length")
    func minhashLength() {
        let sig = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "A sufficiently long transcript for fingerprinting purposes here",
            transcriptReliability: 1.0
        )!
        #expect(sig.minhashSignature.count == MinHashConfig.hashCount)
    }
}

// MARK: - AcousticSignature Tests

@Suite("AcousticSignature")
struct AcousticSignatureTests {

    @Test("Identical acoustic signatures produce similarity of 1.0")
    func identicalSignatures() {
        let sig = AcousticSignature(
            hasMusicBed: true,
            musicBedContour: [0.2, 0.5, 0.8, 0.6, 0.3, 0.4, 0.7, 0.5],
            avgProsodySteadiness: 0.8,
            spectralSketch: [0.1, 0.3, 0.5, 0.7, 0.6, 0.4, 0.2, 0.1]
        )
        let sim = sig.similarity(to: sig)
        #expect(sim > 0.99, "Identical signatures should be ~1.0, got \(sim)")
    }

    @Test("Mismatched music bed reduces similarity")
    func musicBedMismatch() {
        let sig1 = AcousticSignature(
            hasMusicBed: true,
            musicBedContour: nil,
            avgProsodySteadiness: 0.8,
            spectralSketch: nil
        )
        let sig2 = AcousticSignature(
            hasMusicBed: false,
            musicBedContour: nil,
            avgProsodySteadiness: 0.8,
            spectralSketch: nil
        )
        let sim = sig1.similarity(to: sig2)
        // Music bed mismatch loses 0.3/0.6 weight, prosody matches perfectly
        #expect(sim < 0.7, "Music bed mismatch should reduce similarity, got \(sim)")
        #expect(sim > 0.3, "Prosody match should still contribute, got \(sim)")
    }

    @Test("Prosody difference reduces similarity")
    func prosodyDifference() {
        let sig1 = AcousticSignature(
            hasMusicBed: true,
            musicBedContour: nil,
            avgProsodySteadiness: 0.9,
            spectralSketch: nil
        )
        let sig2 = AcousticSignature(
            hasMusicBed: true,
            musicBedContour: nil,
            avgProsodySteadiness: 0.1,
            spectralSketch: nil
        )
        let sim = sig1.similarity(to: sig2)
        #expect(sim < 0.8, "Large prosody difference should reduce similarity, got \(sim)")
    }

    @Test("Contour similarity uses cosine similarity")
    func contourSimilarity() {
        let sig1 = AcousticSignature(
            hasMusicBed: true,
            musicBedContour: [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            avgProsodySteadiness: 0.5,
            spectralSketch: nil
        )
        let sig2 = AcousticSignature(
            hasMusicBed: true,
            musicBedContour: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0],
            avgProsodySteadiness: 0.5,
            spectralSketch: nil
        )
        let sim = sig1.similarity(to: sig2)
        // Orthogonal contours give 0 cosine similarity for the contour component
        // but music bed match and prosody match still contribute
        #expect(sim < 0.9, "Orthogonal contours should reduce score, got \(sim)")
    }

    @Test("Builder clamps prosody to 0-1")
    func prosodyClamping() {
        let sig = CompositeFingerprintBuilder.buildAcousticSignature(
            hasMusicBed: false,
            avgProsodySteadiness: 1.5
        )
        #expect(sig.avgProsodySteadiness == 1.0)

        let sig2 = CompositeFingerprintBuilder.buildAcousticSignature(
            hasMusicBed: false,
            avgProsodySteadiness: -0.5
        )
        #expect(sig2.avgProsodySteadiness == 0.0)
    }
}

// MARK: - SponsorMarkerSignature Tests

@Suite("SponsorMarkerSignature")
struct SponsorMarkerSignatureTests {

    @Test("Identical markers produce similarity of 1.0")
    func identicalMarkers() {
        let sig = SponsorMarkerSignature(
            hasURL: true,
            hasPromoCode: true,
            hasDisclosure: true,
            urlPosition: 0.3,
            promoCodePosition: 0.7,
            disclosurePosition: 0.1
        )
        #expect(sig.similarity(to: sig) == 1.0)
    }

    @Test("All markers mismatched produces low similarity")
    func allMismatch() {
        let sig1 = SponsorMarkerSignature(
            hasURL: true,
            hasPromoCode: true,
            hasDisclosure: true,
            urlPosition: 0.3,
            promoCodePosition: 0.7,
            disclosurePosition: 0.1
        )
        let sig2 = SponsorMarkerSignature(
            hasURL: false,
            hasPromoCode: false,
            hasDisclosure: false,
            urlPosition: nil,
            promoCodePosition: nil,
            disclosurePosition: nil
        )
        let sim = sig1.similarity(to: sig2)
        #expect(sim < 0.1, "All mismatched markers should produce very low similarity, got \(sim)")
    }

    @Test("Position alignment affects score")
    func positionAlignment() {
        let sig1 = SponsorMarkerSignature(
            hasURL: true,
            hasPromoCode: false,
            hasDisclosure: false,
            urlPosition: 0.1,
            promoCodePosition: nil,
            disclosurePosition: nil
        )
        let nearSig = SponsorMarkerSignature(
            hasURL: true,
            hasPromoCode: false,
            hasDisclosure: false,
            urlPosition: 0.12,
            promoCodePosition: nil,
            disclosurePosition: nil
        )
        let farSig = SponsorMarkerSignature(
            hasURL: true,
            hasPromoCode: false,
            hasDisclosure: false,
            urlPosition: 0.9,
            promoCodePosition: nil,
            disclosurePosition: nil
        )
        let nearSim = sig1.similarity(to: nearSig)
        let farSim = sig1.similarity(to: farSig)
        #expect(nearSim > farSim, "Closer positions should score higher")
    }

    @Test("Partial marker match")
    func partialMatch() {
        let sig1 = SponsorMarkerSignature(
            hasURL: true,
            hasPromoCode: true,
            hasDisclosure: false,
            urlPosition: 0.5,
            promoCodePosition: 0.8,
            disclosurePosition: nil
        )
        let sig2 = SponsorMarkerSignature(
            hasURL: true,
            hasPromoCode: false,
            hasDisclosure: false,
            urlPosition: 0.5,
            promoCodePosition: nil,
            disclosurePosition: nil
        )
        let sim = sig1.similarity(to: sig2)
        // 2/3 presence match, 1 position match (URL)
        #expect(sim > 0.5, "Partial match should produce moderate similarity, got \(sim)")
        #expect(sim < 1.0)
    }
}

// MARK: - CompositeFingerprint Tests

@Suite("CompositeFingerprint — Composite Scoring")
struct CompositeFingerprintScoringTests {

    @Test("Identical composite fingerprints score ~1.0")
    func identicalFingerprints() {
        let fp = CompositeFingerprintBuilder.build(
            text: "This episode is brought to you by BetterHelp online therapy",
            transcriptReliability: 1.0,
            hasMusicBed: true,
            avgProsodySteadiness: 0.8,
            hasURL: true,
            hasPromoCode: true,
            hasDisclosure: false,
            urlPosition: 0.3,
            promoCodePosition: 0.7
        )
        let score = fp.compositeScore(against: fp, transcriptReliability: 1.0)
        #expect(score > 0.95, "Identical fingerprints should score ~1.0, got \(score)")
    }

    @Test("Transcript reliability gating — below 0.3 drops text weight to 0")
    func reliabilityGating() {
        let fp1 = CompositeFingerprintBuilder.build(
            text: "This episode is brought to you by BetterHelp",
            transcriptReliability: 1.0,
            hasMusicBed: true,
            avgProsodySteadiness: 0.8
        )
        let fp2 = CompositeFingerprintBuilder.build(
            text: "Completely different unrelated text about weather and sports",
            transcriptReliability: 1.0,
            hasMusicBed: true,
            avgProsodySteadiness: 0.8
        )

        // With high reliability, text mismatch reduces score
        let highRelScore = fp1.compositeScore(against: fp2, transcriptReliability: 1.0)

        // With low reliability, text is ignored — only acoustic matters
        let lowRelScore = fp1.compositeScore(against: fp2, transcriptReliability: 0.2)

        #expect(lowRelScore > highRelScore,
                "Low reliability should ignore text mismatch: low=\(lowRelScore) high=\(highRelScore)")
    }

    @Test("Reliability at boundary 0.3 — just below gets zero text weight")
    func reliabilityBoundary() {
        // Use high text similarity but low acoustic similarity so the text
        // component's presence/absence changes the composite score.
        let fp1 = CompositeFingerprintBuilder.build(
            text: "Same text repeated here for matching purposes in the ad",
            transcriptReliability: 1.0,
            hasMusicBed: true,
            avgProsodySteadiness: 0.9
        )
        let fp2 = CompositeFingerprintBuilder.build(
            text: "Same text repeated here for matching purposes in the ad",
            transcriptReliability: 1.0,
            hasMusicBed: false,
            avgProsodySteadiness: 0.1
        )

        let atBoundary = fp1.compositeScore(against: fp2, transcriptReliability: 0.3)
        let belowBoundary = fp1.compositeScore(against: fp2, transcriptReliability: 0.29)

        // At 0.3, text similarity (1.0) is included and lifts the score.
        // Below 0.3, text is gated out — only low-similarity acoustic remains.
        #expect(atBoundary > belowBoundary,
                "At 0.3 should include text; below should not: at=\(atBoundary) below=\(belowBoundary)")
    }

    @Test("Missing signatures — only available signals used")
    func missingSignatures() {
        // Only transcript
        let fpText = CompositeFingerprint(
            transcriptSignature: CompositeFingerprintBuilder.buildTranscriptSignature(
                text: "Brought to you by Squarespace",
                transcriptReliability: 1.0
            ),
            acousticSignature: nil,
            sponsorMarkers: nil
        )
        let score = fpText.compositeScore(against: fpText, transcriptReliability: 1.0)
        #expect(score > 0.95, "Single signal should still produce high score for identical data, got \(score)")
    }

    @Test("All signatures nil produces score of 0")
    func allNilSignatures() {
        let fp = CompositeFingerprint(
            transcriptSignature: nil,
            acousticSignature: nil,
            sponsorMarkers: nil
        )
        let score = fp.compositeScore(against: fp, transcriptReliability: 1.0)
        #expect(score == 0, "No signals should produce 0 score")
    }

    @Test("Weight normalization — acoustic-only uses full weight")
    func acousticOnlyNormalization() {
        let acoustic = CompositeFingerprintBuilder.buildAcousticSignature(
            hasMusicBed: true,
            avgProsodySteadiness: 0.9
        )
        let fp = CompositeFingerprint(
            transcriptSignature: nil,
            acousticSignature: acoustic,
            sponsorMarkers: nil
        )
        let score = fp.compositeScore(against: fp, transcriptReliability: 1.0)
        // Acoustic-only: weight is 0.3, normalized to 1.0. Identical -> ~1.0
        #expect(score > 0.95, "Acoustic-only identical should score ~1.0, got \(score)")
    }

    @Test("Same-show threshold — score >= 0.7 for matching ad copy")
    func sameShowThreshold() {
        let fp1 = CompositeFingerprintBuilder.build(
            text: "Go to betterhelp dot com slash podcast for ten percent off your first month",
            transcriptReliability: 0.9,
            hasMusicBed: true,
            musicBedContour: [0.2, 0.4, 0.6, 0.5, 0.3, 0.4, 0.5, 0.3],
            avgProsodySteadiness: 0.85,
            hasURL: true,
            hasPromoCode: true,
            hasDisclosure: true,
            urlPosition: 0.3,
            promoCodePosition: 0.7,
            disclosurePosition: 0.05
        )
        // Slightly varied version (ASR differences)
        let fp2 = CompositeFingerprintBuilder.build(
            text: "Go to better help dot com slash podcast for 10 percent off your first month",
            transcriptReliability: 0.9,
            hasMusicBed: true,
            musicBedContour: [0.2, 0.4, 0.6, 0.5, 0.3, 0.4, 0.5, 0.3],
            avgProsodySteadiness: 0.83,
            hasURL: true,
            hasPromoCode: true,
            hasDisclosure: true,
            urlPosition: 0.32,
            promoCodePosition: 0.68,
            disclosurePosition: 0.06
        )
        let score = fp1.compositeScore(against: fp2, transcriptReliability: 0.9)
        #expect(score >= 0.7, "Same ad copy variants should exceed 0.7 threshold, got \(score)")
    }
}

// MARK: - CompositeFingerprintBuilder Tests

@Suite("CompositeFingerprintBuilder")
struct CompositeFingerprintBuilderTests {

    @Test("Build with all features populates all signatures")
    func buildAllFeatures() {
        let fp = CompositeFingerprintBuilder.build(
            text: "Brought to you by Squarespace",
            transcriptReliability: 1.0,
            hasMusicBed: true,
            avgProsodySteadiness: 0.7,
            hasURL: true,
            hasPromoCode: false,
            hasDisclosure: true,
            urlPosition: 0.4,
            disclosurePosition: 0.1
        )
        #expect(fp.transcriptSignature != nil)
        #expect(fp.acousticSignature != nil)
        #expect(fp.sponsorMarkers != nil)
    }

    @Test("Build with no features produces empty composite")
    func buildNoFeatures() {
        let fp = CompositeFingerprintBuilder.build()
        #expect(fp.transcriptSignature == nil)
        #expect(fp.acousticSignature == nil)
        #expect(fp.sponsorMarkers == nil)
    }

    @Test("Build with low reliability skips transcript signature")
    func buildLowReliability() {
        let fp = CompositeFingerprintBuilder.build(
            text: "Some text",
            transcriptReliability: 0.1,
            hasMusicBed: false,
            avgProsodySteadiness: 0.5
        )
        #expect(fp.transcriptSignature == nil, "Low reliability should skip transcript signature")
        #expect(fp.acousticSignature != nil, "Acoustic should still be built")
    }

    @Test("Sponsor marker builder strips positions when marker absent")
    func markerPositionStripping() {
        let sig = CompositeFingerprintBuilder.buildSponsorMarkerSignature(
            hasURL: false,
            hasPromoCode: true,
            hasDisclosure: false,
            urlPosition: 0.5,  // should be stripped
            promoCodePosition: 0.7,
            disclosurePosition: 0.1  // should be stripped
        )
        #expect(sig.urlPosition == nil, "Position should be nil when marker absent")
        #expect(sig.promoCodePosition == 0.7)
        #expect(sig.disclosurePosition == nil, "Position should be nil when marker absent")
    }

    @Test("Acoustic builder requires both hasMusicBed and prosody")
    func acousticRequiresBothParams() {
        // Only prosody, no hasMusicBed -> no acoustic signature
        let fp = CompositeFingerprintBuilder.build(
            avgProsodySteadiness: 0.5
        )
        #expect(fp.acousticSignature == nil)
    }

    @Test("Marker builder requires all three presence flags")
    func markerRequiresAllFlags() {
        // Only hasURL, missing others -> no marker signature
        let fp = CompositeFingerprintBuilder.build(
            hasURL: true
        )
        #expect(fp.sponsorMarkers == nil)
    }
}

// MARK: - Edge Cases

@Suite("CompositeFingerprint — Edge Cases")
struct CompositeFingerprintEdgeCaseTests {

    @Test("Sendable conformance compiles")
    func sendableConformance() {
        let fp: any Sendable = CompositeFingerprintBuilder.build(
            text: "test",
            transcriptReliability: 1.0
        )
        #expect(fp is CompositeFingerprint)
    }

    @Test("TranscriptSignature Equatable")
    func transcriptEquatable() {
        let sig1 = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "identical text",
            transcriptReliability: 1.0
        )!
        let sig2 = CompositeFingerprintBuilder.buildTranscriptSignature(
            text: "identical text",
            transcriptReliability: 1.0
        )!
        #expect(sig1 == sig2)
    }

    @Test("CompositeFingerprint Equatable")
    func compositeEquatable() {
        let fp1 = CompositeFingerprintBuilder.build(
            text: "test",
            transcriptReliability: 1.0,
            hasMusicBed: true,
            avgProsodySteadiness: 0.5,
            hasURL: true,
            hasPromoCode: false,
            hasDisclosure: false
        )
        let fp2 = CompositeFingerprintBuilder.build(
            text: "test",
            transcriptReliability: 1.0,
            hasMusicBed: true,
            avgProsodySteadiness: 0.5,
            hasURL: true,
            hasPromoCode: false,
            hasDisclosure: false
        )
        #expect(fp1 == fp2)
    }

    @Test("Composite score with reliability exactly 0.0")
    func zeroReliability() {
        let fp = CompositeFingerprintBuilder.build(
            text: "some text here for testing",
            transcriptReliability: 1.0,
            hasMusicBed: false,
            avgProsodySteadiness: 0.5
        )
        let score = fp.compositeScore(against: fp, transcriptReliability: 0.0)
        // Text should be fully gated out, only acoustic contributes
        #expect(score > 0.5, "Acoustic should still contribute, got \(score)")
    }

    @Test("Composite score with reliability exactly 1.0")
    func fullReliability() {
        let fp = CompositeFingerprintBuilder.build(
            text: "this is a test of the composite fingerprint system",
            transcriptReliability: 1.0,
            hasMusicBed: true,
            avgProsodySteadiness: 0.7
        )
        let score = fp.compositeScore(against: fp, transcriptReliability: 1.0)
        #expect(score > 0.95, "Full reliability identical should be ~1.0, got \(score)")
    }

    @Test("Empty spectral sketch and contour arrays handled gracefully")
    func emptyArrays() {
        let sig1 = AcousticSignature(
            hasMusicBed: true,
            musicBedContour: [],
            avgProsodySteadiness: 0.5,
            spectralSketch: []
        )
        let sig2 = AcousticSignature(
            hasMusicBed: true,
            musicBedContour: [],
            avgProsodySteadiness: 0.5,
            spectralSketch: []
        )
        // Should not crash; empty arrays skipped
        let sim = sig1.similarity(to: sig2)
        #expect(sim > 0.5, "Should handle empty arrays gracefully, got \(sim)")
    }

    @Test("Mismatched contour lengths handled via cosine similarity min-length")
    func mismatchedContourLengths() {
        let sig1 = AcousticSignature(
            hasMusicBed: true,
            musicBedContour: [0.5, 0.5, 0.5, 0.5],
            avgProsodySteadiness: 0.5,
            spectralSketch: nil
        )
        let sig2 = AcousticSignature(
            hasMusicBed: true,
            musicBedContour: [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
            avgProsodySteadiness: 0.5,
            spectralSketch: nil
        )
        // Should not crash
        let sim = sig1.similarity(to: sig2)
        #expect(sim > 0.5)
    }
}
