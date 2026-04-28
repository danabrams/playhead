// TrainingExampleBucketer.swift
// playhead-4my.10.1: pure bucket-classification helper used by the
// `TrainingExampleMaterializer`. All ledger reading happens upstream — by
// the time the bucketer runs, the per-region signals have already been
// summarized into a `TrainingExampleBucketerSignals` value. Keeping the
// bucketer free of I/O makes it cheap to table-test against every
// disagreement subtype the bead calls out.

import Foundation

/// Pre-summarized per-region signals used by the bucketer. The
/// materializer is responsible for collapsing the raw evidence /
/// decision / correction ledgers down to this shape.
struct TrainingExampleBucketerSignals: Sendable, Equatable {
    /// True when at least one FM evidence event in the region had a
    /// positive disposition (containsAd or refined-span emit).
    let fmPositive: Bool
    /// Highest FM certainty observed in the region. `0..1`.
    let fmCertainty: Double
    /// True when the lexical scanner produced any evidence for the
    /// region. The lexicon only emits rows for matches, so silence
    /// does NOT mean negative — it means "no opinion". M1: required
    /// to separate genuine lexical-vs-FM disagreement (both fired,
    /// disagreed) from "lexicon happened to be quiet".
    let lexicalFired: Bool
    /// True when the lexical scanner's emitted evidence (if any)
    /// flagged the region as ad-like. Implies `lexicalFired`. When
    /// `lexicalFired == false`, this field is unused and should be
    /// `false` by convention.
    let lexicalPositive: Bool
    /// Best (post-fusion) classifier confidence stamped on the decision
    /// for the region. `0..1`.
    let classifierConfidence: Double
    /// True when the post-fusion decision said skip-eligible.
    let decisionWasSkipEligible: Bool
    /// User tapped Listen / This isn't an ad inside this region.
    let userReverted: Bool
    /// User reported a missed ad ("Hearing an ad" / mark-as-ad) inside
    /// this region.
    let userReportedFalseNegative: Bool
    /// Persistence-layer transcript quality string
    /// ("good" | "degraded" | "unusable").
    let transcriptQuality: String

    init(
        fmPositive: Bool,
        fmCertainty: Double,
        lexicalFired: Bool,
        lexicalPositive: Bool,
        classifierConfidence: Double,
        decisionWasSkipEligible: Bool,
        userReverted: Bool,
        userReportedFalseNegative: Bool,
        transcriptQuality: String
    ) {
        self.fmPositive = fmPositive
        self.fmCertainty = fmCertainty
        self.lexicalFired = lexicalFired
        self.lexicalPositive = lexicalPositive
        self.classifierConfidence = classifierConfidence
        self.decisionWasSkipEligible = decisionWasSkipEligible
        self.userReverted = userReverted
        self.userReportedFalseNegative = userReportedFalseNegative
        self.transcriptQuality = transcriptQuality
    }
}

/// Pure bucket classifier. Branches in priority order so the most
/// information-dense bucket (`.disagreement`) wins on conflicting evidence.
enum TrainingExampleBucketer {

    /// Maps per-region signals to a single training-example bucket.
    ///
    /// Priority order (top-down):
    ///   1. `.disagreement` — any of:
    ///        * model-vs-user: FM-positive but user reverted
    ///        * model-vs-user: FM-negative but user reported a missed ad
    ///        * lexical-vs-FM: lexical and FM disagree (good transcript only,
    ///          so we don't confuse "model can't tell because it's noise"
    ///          with a real disagreement)
    ///   2. `.uncertain` — transcript unusable, or no positive signal of
    ///        either kind AND no user feedback to anchor the label.
    ///   3. `.positive`   — FM-positive AND high certainty (or
    ///        lexical-corroborated) AND user did not revert.
    ///   4. `.negative`   — everything else (decision said no, no user
    ///        signal contradicting it).
    static func bucket(for signals: TrainingExampleBucketerSignals) -> TrainingExampleBucket {

        // 1. Disagreement subtypes.
        // 1a. Model-vs-user: user clearly contradicts the model.
        if signals.fmPositive && signals.userReverted {
            return .disagreement
        }
        if !signals.fmPositive && signals.userReportedFalseNegative {
            return .disagreement
        }

        // 1b. Lexical-vs-FM: only on good transcripts. On unusable
        // transcripts the disagreement is more likely an ASR artifact
        // than a real conflict, so we'd rather emit .uncertain.
        //
        // M1: require that BOTH systems actually fired. The lexical
        // scanner only emits evidence for positive matches — silence
        // is "no opinion", not a negative verdict. Treating "FM said
        // ad, lexicon was quiet" as a disagreement floods the bucket
        // with the most common case (many real ads don't trip the
        // lexicon) and dilutes its training value. We only flag a
        // genuine disagreement when both systems produced an opinion
        // and those opinions conflict.
        if signals.transcriptQuality == "good"
            && signals.lexicalFired
            && signals.fmPositive != signals.lexicalPositive {
            return .disagreement
        }

        // 2. Uncertain.
        if signals.transcriptQuality == "unusable" {
            return .uncertain
        }
        // On non-good transcripts (e.g. "degraded"), absence of any
        // positive signal is genuinely ambiguous — the ASR could simply
        // have missed lexical cues — so we route those to .uncertain.
        // On a "good" transcript the model has had every opportunity
        // to flag the region and chose not to; we treat that as a
        // usable confirmed-negative example below.
        let hasAnyPositiveSignal =
            (signals.fmPositive && signals.fmCertainty >= 0.5) ||
            signals.lexicalPositive ||
            signals.classifierConfidence >= 0.6 ||
            signals.userReverted ||
            signals.userReportedFalseNegative
        if signals.transcriptQuality != "good"
            && !hasAnyPositiveSignal
            && !signals.decisionWasSkipEligible {
            return .uncertain
        }

        // 3. Positive: model said yes, user didn't push back.
        if signals.fmPositive && signals.fmCertainty >= 0.7 && !signals.userReverted {
            return .positive
        }
        if signals.decisionWasSkipEligible && !signals.userReverted {
            return .positive
        }

        // 4. Negative: everything else (model said no, user agreed).
        return .negative
    }
}
