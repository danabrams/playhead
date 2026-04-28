// TrainingExampleBucketerTests.swift
// playhead-4my.10.1: bucket-classification logic for materializing training
// examples. The bucketer takes the per-region signals already in the ledger
// and emits one of four buckets:
//
//   - .positive     — high-confidence ad, user did not revert
//   - .negative     — correctly rejected non-ad
//   - .uncertain    — unresolved or low-quality region
//   - .disagreement — lexical-vs-FM, model-vs-user, or FM-positive-but-user-reverted
//
// Per the bead: the disagreement bucket is the most valuable for training,
// so we make sure every documented disagreement subtype produces it.

import Foundation
import Testing

@testable import Playhead

@Suite("TrainingExampleBucketer — playhead-4my.10.1")
struct TrainingExampleBucketerTests {

    // MARK: - Fixture builder (cycle-2 L-E)

    /// cycle-2 L-E: a single fixture builder for `TrainingExampleBucketerSignals`
    /// with sensible defaults. Each test overrides only the fields it actually
    /// exercises so a future bucketer signal can be added by extending the
    /// builder once instead of touching every fixture site. Defaults are the
    /// "neutral negative" shape: a clean transcript with no positive signal,
    /// no user feedback, and no decision.
    private static func signals(
        fmPositive: Bool = false,
        fmCertainty: Double = 0.0,
        lexicalFired: Bool = false,
        lexicalPositive: Bool = false,
        classifierConfidence: Double = 0.0,
        decisionWasSkipEligible: Bool = false,
        userReverted: Bool = false,
        userReportedFalseNegative: Bool = false,
        transcriptQuality: String = "good"
    ) -> TrainingExampleBucketerSignals {
        TrainingExampleBucketerSignals(
            fmPositive: fmPositive,
            fmCertainty: fmCertainty,
            lexicalFired: lexicalFired,
            lexicalPositive: lexicalPositive,
            classifierConfidence: classifierConfidence,
            decisionWasSkipEligible: decisionWasSkipEligible,
            userReverted: userReverted,
            userReportedFalseNegative: userReportedFalseNegative,
            transcriptQuality: transcriptQuality
        )
    }

    // MARK: - .positive

    @Test("high-confidence FM + lexical, user did not revert -> .positive")
    func highConfidenceConfirmedAd() {
        let s = Self.signals(
            fmPositive: true,
            fmCertainty: 0.95,
            lexicalFired: true,
            lexicalPositive: true,
            classifierConfidence: 0.85,
            decisionWasSkipEligible: true
        )
        #expect(TrainingExampleBucketer.bucket(for: s) == .positive)
    }

    // MARK: - .negative

    @Test("FM-negative + lexical silent + no correction -> .negative")
    func confirmedNonAd() {
        let s = Self.signals(
            fmCertainty: 0.05,
            classifierConfidence: 0.1
        )
        #expect(TrainingExampleBucketer.bucket(for: s) == .negative)
    }

    // MARK: - .uncertain

    @Test("low-quality transcript -> .uncertain")
    func unusableTranscriptIsUncertain() {
        let s = Self.signals(
            fmPositive: true,
            fmCertainty: 0.6,
            lexicalFired: true,
            lexicalPositive: true,
            classifierConfidence: 0.5,
            transcriptQuality: "unusable"
        )
        #expect(TrainingExampleBucketer.bucket(for: s) == .uncertain)
    }

    @Test("low FM certainty + low classifier conf + no user signal -> .uncertain")
    func lowConfidenceMixedSignalsIsUncertain() {
        let s = Self.signals(
            fmCertainty: 0.4,
            classifierConfidence: 0.4,
            // borderline quality but not unusable
            transcriptQuality: "degraded"
        )
        #expect(TrainingExampleBucketer.bucket(for: s) == .uncertain)
    }

    // MARK: - .disagreement

    @Test("lexical fired positive but FM-negative -> .disagreement (lexical-vs-FM)")
    func lexicalVsFMDisagreement() {
        let s = Self.signals(
            fmCertainty: 0.2,
            lexicalFired: true,
            lexicalPositive: true,
            classifierConfidence: 0.6
        )
        #expect(TrainingExampleBucketer.bucket(for: s) == .disagreement)
    }

    @Test("FM-positive but user reverted -> .disagreement (model-vs-user)")
    func fmPositiveUserRevertedIsDisagreement() {
        let s = Self.signals(
            fmPositive: true,
            fmCertainty: 0.95,
            lexicalFired: true,
            lexicalPositive: true,
            classifierConfidence: 0.8,
            decisionWasSkipEligible: true,
            userReverted: true
        )
        #expect(TrainingExampleBucketer.bucket(for: s) == .disagreement)
    }

    @Test("FM-negative but user reported false negative -> .disagreement")
    func userReportedMissedAdIsDisagreement() {
        let s = Self.signals(
            fmCertainty: 0.1,
            classifierConfidence: 0.2,
            userReportedFalseNegative: true
        )
        #expect(TrainingExampleBucketer.bucket(for: s) == .disagreement)
    }

    // MARK: - M1: lexicon silence is NOT a disagreement

    @Test("FM-positive, lexicon silent -> NOT .disagreement (lexicon was quiet, not negative)")
    func fmPositiveLexiconSilentIsNotDisagreement() {
        // Common case: many real ads don't trip the lexicon. Pre-M1 this
        // produced a flood of false-disagreement entries; post-M1 we
        // require the lexicon to have actually fired before declaring a
        // lexical-vs-FM disagreement.
        let s = Self.signals(
            fmPositive: true,
            fmCertainty: 0.85,
            classifierConfidence: 0.7,
            decisionWasSkipEligible: true
        )
        // Should be .positive (FM strong + skip-eligible), not .disagreement.
        #expect(TrainingExampleBucketer.bucket(for: s) == .positive)
    }

    @Test("FM-negative, lexicon silent -> .negative (no disagreement when both quiet)")
    func fmNegativeLexiconSilentIsNegative() {
        let s = Self.signals(
            classifierConfidence: 0.1
        )
        #expect(TrainingExampleBucketer.bucket(for: s) == .negative)
    }
}
