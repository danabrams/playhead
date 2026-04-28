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

    // MARK: - .positive

    @Test("high-confidence FM + lexical, user did not revert -> .positive")
    func highConfidenceConfirmedAd() {
        let signals = TrainingExampleBucketerSignals(
            fmPositive: true,
            fmCertainty: 0.95,
            lexicalFired: true,
            lexicalPositive: true,
            classifierConfidence: 0.85,
            decisionWasSkipEligible: true,
            userReverted: false,
            userReportedFalseNegative: false,
            transcriptQuality: "good"
        )
        #expect(TrainingExampleBucketer.bucket(for: signals) == .positive)
    }

    // MARK: - .negative

    @Test("FM-negative + lexical silent + no correction -> .negative")
    func confirmedNonAd() {
        let signals = TrainingExampleBucketerSignals(
            fmPositive: false,
            fmCertainty: 0.05,
            lexicalFired: false,
            lexicalPositive: false,
            classifierConfidence: 0.1,
            decisionWasSkipEligible: false,
            userReverted: false,
            userReportedFalseNegative: false,
            transcriptQuality: "good"
        )
        #expect(TrainingExampleBucketer.bucket(for: signals) == .negative)
    }

    // MARK: - .uncertain

    @Test("low-quality transcript -> .uncertain")
    func unusableTranscriptIsUncertain() {
        let signals = TrainingExampleBucketerSignals(
            fmPositive: true,
            fmCertainty: 0.6,
            lexicalFired: true,
            lexicalPositive: true,
            classifierConfidence: 0.5,
            decisionWasSkipEligible: false,
            userReverted: false,
            userReportedFalseNegative: false,
            transcriptQuality: "unusable"
        )
        #expect(TrainingExampleBucketer.bucket(for: signals) == .uncertain)
    }

    @Test("low FM certainty + low classifier conf + no user signal -> .uncertain")
    func lowConfidenceMixedSignalsIsUncertain() {
        let signals = TrainingExampleBucketerSignals(
            fmPositive: false,
            fmCertainty: 0.4,
            lexicalFired: false,
            lexicalPositive: false,
            classifierConfidence: 0.4,
            decisionWasSkipEligible: false,
            userReverted: false,
            userReportedFalseNegative: false,
            // borderline quality but not unusable
            transcriptQuality: "degraded"
        )
        #expect(TrainingExampleBucketer.bucket(for: signals) == .uncertain)
    }

    // MARK: - .disagreement

    @Test("lexical fired positive but FM-negative -> .disagreement (lexical-vs-FM)")
    func lexicalVsFMDisagreement() {
        let signals = TrainingExampleBucketerSignals(
            fmPositive: false,
            fmCertainty: 0.2,
            lexicalFired: true,
            lexicalPositive: true,
            classifierConfidence: 0.6,
            decisionWasSkipEligible: false,
            userReverted: false,
            userReportedFalseNegative: false,
            transcriptQuality: "good"
        )
        #expect(TrainingExampleBucketer.bucket(for: signals) == .disagreement)
    }

    @Test("FM-positive but user reverted -> .disagreement (model-vs-user)")
    func fmPositiveUserRevertedIsDisagreement() {
        let signals = TrainingExampleBucketerSignals(
            fmPositive: true,
            fmCertainty: 0.95,
            lexicalFired: true,
            lexicalPositive: true,
            classifierConfidence: 0.8,
            decisionWasSkipEligible: true,
            userReverted: true,
            userReportedFalseNegative: false,
            transcriptQuality: "good"
        )
        #expect(TrainingExampleBucketer.bucket(for: signals) == .disagreement)
    }

    @Test("FM-negative but user reported false negative -> .disagreement")
    func userReportedMissedAdIsDisagreement() {
        let signals = TrainingExampleBucketerSignals(
            fmPositive: false,
            fmCertainty: 0.1,
            lexicalFired: false,
            lexicalPositive: false,
            classifierConfidence: 0.2,
            decisionWasSkipEligible: false,
            userReverted: false,
            userReportedFalseNegative: true,
            transcriptQuality: "good"
        )
        #expect(TrainingExampleBucketer.bucket(for: signals) == .disagreement)
    }

    // MARK: - M1: lexicon silence is NOT a disagreement

    @Test("FM-positive, lexicon silent -> NOT .disagreement (lexicon was quiet, not negative)")
    func fmPositiveLexiconSilentIsNotDisagreement() {
        // Common case: many real ads don't trip the lexicon. Pre-M1 this
        // produced a flood of false-disagreement entries; post-M1 we
        // require the lexicon to have actually fired before declaring a
        // lexical-vs-FM disagreement.
        let signals = TrainingExampleBucketerSignals(
            fmPositive: true,
            fmCertainty: 0.85,
            lexicalFired: false,
            lexicalPositive: false,
            classifierConfidence: 0.7,
            decisionWasSkipEligible: true,
            userReverted: false,
            userReportedFalseNegative: false,
            transcriptQuality: "good"
        )
        // Should be .positive (FM strong + skip-eligible), not .disagreement.
        #expect(TrainingExampleBucketer.bucket(for: signals) == .positive)
    }

    @Test("FM-negative, lexicon silent -> .negative (no disagreement when both quiet)")
    func fmNegativeLexiconSilentIsNegative() {
        let signals = TrainingExampleBucketerSignals(
            fmPositive: false,
            fmCertainty: 0.0,
            lexicalFired: false,
            lexicalPositive: false,
            classifierConfidence: 0.1,
            decisionWasSkipEligible: false,
            userReverted: false,
            userReportedFalseNegative: false,
            transcriptQuality: "good"
        )
        #expect(TrainingExampleBucketer.bucket(for: signals) == .negative)
    }
}
