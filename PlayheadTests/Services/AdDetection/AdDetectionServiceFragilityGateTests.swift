// AdDetectionServiceFragilityGateTests.swift
// playhead-xsdz.7: Tests for the Evidence-Fragility precision gate — a
// deterministic, post-fusion SOFT penalty on `skipConfidence` for spans whose
// evidence geometry looks brittle (one dominant channel, narrow margin over
// the track threshold, few distinct evidence families).
//
// The scoring lives in `AdDetectionConfig.applyFragilityPenalty(...)` /
// `fragilityScore(...)` — pure helpers, so we test them directly without
// spinning up the actor (mirrors `AdDetectionServicePromotionGateTests`).

import Foundation
import Testing
@testable import Playhead

@Suite("AdDetectionService fragility gate (playhead-xsdz.7)")
struct AdDetectionServiceFragilityGateTests {

    // MARK: - Helpers

    /// Build a config with the fragility knobs explicitly set. Standard /
    /// qualified auto-skip thresholds default to production values so the
    /// margin term is realistic.
    private func makeConfig(
        enabled: Bool = false,
        threshold: Double = 2.0,
        penalty: Double = 0.85,
        autoSkip: Double = 0.80,
        classifierSeedQualified: Double = 0.50,
        lexicalAutoAdQualified: Double = 0.50
    ) -> AdDetectionConfig {
        AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "xsdz7-test",
            autoSkipConfidenceThreshold: autoSkip,
            classifierSeedQualifiedThreshold: classifierSeedQualified,
            lexicalAutoAdQualifiedThreshold: lexicalAutoAdQualified,
            evidenceFragilityPenaltyEnabled: enabled,
            fragilityThreshold: threshold,
            fragilityPenalty: penalty
        )
    }

    private func entry(_ source: EvidenceSourceType, _ weight: Double) -> EvidenceLedgerEntry {
        EvidenceLedgerEntry(
            source: source,
            weight: weight,
            detail: detail(for: source)
        )
    }

    /// A minimal valid `EvidenceLedgerDetail` per source — only `source` and
    /// `weight` matter to the fragility geometry; the detail payload is inert.
    private func detail(for source: EvidenceSourceType) -> EvidenceLedgerDetail {
        switch source {
        case .classifier, .fusedScore:
            return .classifier(score: 0.5)
        case .fm:
            return .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "t")
        case .lexical, .lexicalAutoAd:
            return .lexical(matchedCategories: ["url"])
        case .acoustic:
            return .acoustic(breakStrength: 0.5)
        case .breakAlignment:
            return .breakAlignment(breakStrength: 0.5)
        case .catalog:
            return .catalog(entryCount: 1)
        case .fingerprint:
            return .fingerprint(matchCount: 1, averageSimilarity: 0.5)
        case .metadata:
            return .metadata(cueCount: 1, sourceField: .description, dominantCueType: .disclosure)
        case .musicBed:
            return .musicBed(presenceFraction: 0.5, foregroundCount: 1)
        case .audit, .operational:
            // Observability-only rows are filtered out before scoring; the
            // detail shape is irrelevant. Use a benign classifier payload.
            return .classifier(score: 0.0)
        }
    }

    /// A brittle ledger: one dominant FM (`.model` family) entry, nothing
    /// else with weight. Combined with a narrow margin this is the canonical
    /// false-positive shape the gate targets.
    private func fragileLedger() -> [EvidenceLedgerEntry] {
        [
            entry(.classifier, 0.0),   // always-present zero-weight row (ignored: weight == 0)
            entry(.fm, 0.45)           // single dominant channel
        ]
    }

    /// A robust ledger: four distinct families, no single channel dominating.
    private func robustLedger() -> [EvidenceLedgerEntry] {
        [
            entry(.lexical, 0.20),     // textual
            entry(.acoustic, 0.20),    // acoustic
            entry(.fm, 0.30),          // model
            entry(.catalog, 0.20)      // reference
        ]
    }

    // MARK: - (a) Flag-off identity

    @Test("Flag OFF (default): applyFragilityPenalty returns skipConfidence unchanged for a fragile span")
    func flagOffIsIdentityOnFragileSpan() {
        let config = makeConfig(enabled: false)
        let result = config.applyFragilityPenalty(
            skipConfidence: 0.55,
            proposalConfidence: 0.55,
            promotionTrack: .classifierSeedQualified,
            ledger: fragileLedger()
        )
        #expect(result == 0.55,
                "With the flag off the decision must be byte-identical to pre-xsdz.7")
    }

    @Test("AdDetectionConfig.default keeps the fragility gate OFF with documented defaults")
    func configDefaultsAreOffAndConservative() {
        let config = AdDetectionConfig.default
        #expect(config.evidenceFragilityPenaltyEnabled == false,
                "OFF-by-default is load-bearing: main must stay behavior-neutral")
        #expect(config.fragilityThreshold == 2.0)
        #expect(config.fragilityPenalty == 0.85)
    }

    @Test("Flag OFF leaves a fragile span identical across the full confidence range")
    func flagOffIdentityAcrossRange() {
        let config = makeConfig(enabled: false)
        for sc in stride(from: 0.0, through: 1.0, by: 0.1) {
            let out = config.applyFragilityPenalty(
                skipConfidence: sc,
                proposalConfidence: sc,
                promotionTrack: .standard,
                ledger: fragileLedger()
            )
            #expect(out == sc, "flag-off must be identity at skipConfidence=\(sc)")
        }
    }

    // MARK: - (b) Fragile span is penalized when enabled

    @Test("Enabled: a fragile span (dominant channel, narrow margin, one family) is penalized")
    func fragileSpanPenalizedWhenEnabled() {
        let config = makeConfig(enabled: true, threshold: 2.0, penalty: 0.85)
        // margin = 0.55 - 0.50 = 0.05; concentration = 0.45/0.55 ≈ 0.818;
        // depth = 1 (only .model). fragility ≈ 16.4 ≫ 2.0 → penalized.
        let score = config.fragilityScore(
            proposalConfidence: 0.55,
            promotionTrack: .classifierSeedQualified,
            ledger: fragileLedger()
        )
        #expect(score > config.fragilityThreshold,
                "fragile geometry must exceed the threshold (got \(score))")

        let result = config.applyFragilityPenalty(
            skipConfidence: 0.55,
            proposalConfidence: 0.55,
            promotionTrack: .classifierSeedQualified,
            ledger: fragileLedger()
        )
        #expect(result == 0.55 * 0.85,
                "penalized confidence must be skipConfidence * fragilityPenalty (got \(result))")
        #expect(result < 0.55, "penalty must lower the confidence")
    }

    @Test("The penalty can drop a span that just cleared its threshold back below it")
    func penaltyFlipsAMarginalSpanBelowThreshold() {
        // Qualified track threshold 0.50; a fragile span at exactly 0.55
        // clears it pre-penalty but the 0.85 multiplier drops it to ~0.4675.
        let config = makeConfig(enabled: true, threshold: 2.0, penalty: 0.85,
                                classifierSeedQualified: 0.50)
        let before = 0.55
        let after = config.applyFragilityPenalty(
            skipConfidence: before,
            proposalConfidence: before,
            promotionTrack: .classifierSeedQualified,
            ledger: fragileLedger()
        )
        let threshold = config.effectiveAutoSkipThreshold(for: .classifierSeedQualified)
        #expect(before >= threshold, "precondition: span cleared the gate before the penalty")
        #expect(after < threshold, "the soft penalty pushed the brittle span back below the gate")
    }

    // MARK: - (c) Robust span is essentially unaffected when enabled

    @Test("Enabled: a robust span (many families, wide margin) is NOT penalized")
    func robustSpanUnaffectedWhenEnabled() {
        let config = makeConfig(enabled: true, threshold: 2.0, penalty: 0.85)
        // margin = 0.90 - 0.80 = 0.10; concentration = 0.30/0.90 ≈ 0.333;
        // depth = 4. fragility ≈ 0.833 < 2.0 → unchanged.
        let score = config.fragilityScore(
            proposalConfidence: 0.90,
            promotionTrack: .standard,
            ledger: robustLedger()
        )
        #expect(score <= config.fragilityThreshold,
                "robust geometry must stay at/below the threshold (got \(score))")

        let result = config.applyFragilityPenalty(
            skipConfidence: 0.90,
            proposalConfidence: 0.90,
            promotionTrack: .standard,
            ledger: robustLedger()
        )
        #expect(result == 0.90,
                "a robust span must be returned unchanged even with the flag on (got \(result))")
    }

    // MARK: - (d) Formula edge cases — no crash, no divide-by-zero, finite output

    @Test("Edge: zero margin (proposalConfidence == threshold) is guarded by epsilon and stays finite")
    func zeroMarginIsFinite() {
        let config = makeConfig(enabled: true)
        // proposalConfidence exactly at the standard threshold → margin = 0.
        let score = config.fragilityScore(
            proposalConfidence: 0.80,
            promotionTrack: .standard,
            ledger: fragileLedger()
        )
        #expect(score.isFinite, "zero margin must not produce inf/NaN")
        #expect(score > 0)
    }

    @Test("Edge: negative margin (below threshold) is clamped to epsilon, stays finite")
    func negativeMarginIsFinite() {
        let config = makeConfig(enabled: true)
        // proposalConfidence below the standard threshold → margin < 0.
        let score = config.fragilityScore(
            proposalConfidence: 0.30,
            promotionTrack: .standard,
            ledger: fragileLedger()
        )
        #expect(score.isFinite, "negative margin must be clamped, never producing inf/NaN")
        // A sub-threshold span is maximally brittle; result is large but finite.
        let result = config.applyFragilityPenalty(
            skipConfidence: 0.30,
            proposalConfidence: 0.30,
            promotionTrack: .standard,
            ledger: fragileLedger()
        )
        #expect(result.isFinite && result >= 0 && result <= 1)
    }

    @Test("Edge: empty ledger (depth 0, no weighted entries) → zero concentration, finite, no penalty")
    func emptyLedgerIsFinite() {
        let config = makeConfig(enabled: true)
        let score = config.fragilityScore(
            proposalConfidence: 0.55,
            promotionTrack: .standard,
            ledger: []
        )
        #expect(score == 0.0, "no weighted entries ⇒ concentration 0 ⇒ fragility 0")
        let result = config.applyFragilityPenalty(
            skipConfidence: 0.55,
            proposalConfidence: 0.55,
            promotionTrack: .standard,
            ledger: []
        )
        #expect(result == 0.55, "a zero fragility score must not trip the penalty")
    }

    @Test("Edge: only a zero-weight classifier row (depth 0) is finite and unpenalized")
    func onlyZeroWeightClassifierIsFinite() {
        let config = makeConfig(enabled: true)
        let ledger = [entry(.classifier, 0.0)]
        let score = config.fragilityScore(
            proposalConfidence: 0.55,
            promotionTrack: .standard,
            ledger: ledger
        )
        #expect(score == 0.0, "weight == 0 entries are excluded from concentration AND depth")
        let result = config.applyFragilityPenalty(
            skipConfidence: 0.55,
            proposalConfidence: 0.55,
            promotionTrack: .standard,
            ledger: ledger
        )
        #expect(result == 0.55)
    }

    @Test("Edge: zero proposalConfidence is guarded by epsilon (concentration stays finite)")
    func zeroProposalConfidenceIsFinite() {
        let config = makeConfig(enabled: true)
        let score = config.fragilityScore(
            proposalConfidence: 0.0,
            promotionTrack: .standard,
            ledger: fragileLedger()
        )
        #expect(score.isFinite, "zero proposalConfidence must not divide-by-zero")
    }

    @Test("Edge: observability-only rows never count toward concentration or depth")
    func observabilityRowsExcluded() {
        let config = makeConfig(enabled: true)
        // A huge-weight audit row must be ignored entirely.
        let ledger = [
            entry(.fm, 0.45),
            entry(.audit, 0.99),
            entry(.operational, 0.99)
        ]
        let withObservability = config.fragilityScore(
            proposalConfidence: 0.55,
            promotionTrack: .classifierSeedQualified,
            ledger: ledger
        )
        let withoutObservability = config.fragilityScore(
            proposalConfidence: 0.55,
            promotionTrack: .classifierSeedQualified,
            ledger: [entry(.fm, 0.45)]
        )
        #expect(withObservability == withoutObservability,
                "audit/operational rows must not change the fragility geometry")
    }

    @Test("Edge: same-family entries (acoustic + musicBed + breakAlignment) count as depth 1")
    func sameFamilyEntriesCollapseToOneDepth() {
        let config = makeConfig(enabled: true)
        // All three are the .acoustic family per SourceEvidenceFamily.for —
        // depth must be 1, not 3, keeping the depth term honest.
        let audioOnly = [
            entry(.acoustic, 0.20),
            entry(.musicBed, 0.20),
            entry(.breakAlignment, 0.20)
        ]
        // Compare against a single acoustic entry of equal max weight: depth
        // is identical (1), so the only difference in fragility would come
        // from depth — which must be the same here.
        let singleAcoustic = [entry(.acoustic, 0.20)]
        let depthMulti = config.fragilityScore(
            proposalConfidence: 0.60, promotionTrack: .standard, ledger: audioOnly
        )
        let depthSingle = config.fragilityScore(
            proposalConfidence: 0.60, promotionTrack: .standard, ledger: singleAcoustic
        )
        #expect(depthMulti == depthSingle,
                "three audio-derived signals are one family → same depth → same fragility")
    }

    @Test("Edge: fragilityPenalty out of [0,1] is clamped (negative ⇒ 0, >1 ⇒ 1)")
    func penaltyClampedToUnitInterval() {
        // penalty < 0 → clamped to 0 → confidence floored to 0.
        let lo = makeConfig(enabled: true, threshold: 2.0, penalty: -5.0)
        let loResult = lo.applyFragilityPenalty(
            skipConfidence: 0.55, proposalConfidence: 0.55,
            promotionTrack: .classifierSeedQualified, ledger: fragileLedger()
        )
        #expect(loResult == 0.0, "negative penalty clamps to 0")

        // penalty > 1 → clamped to 1 → no boost above the input.
        let hi = makeConfig(enabled: true, threshold: 2.0, penalty: 5.0)
        let hiResult = hi.applyFragilityPenalty(
            skipConfidence: 0.55, proposalConfidence: 0.55,
            promotionTrack: .classifierSeedQualified, ledger: fragileLedger()
        )
        #expect(hiResult == 0.55, "penalty > 1 clamps to 1 — the gate never boosts confidence")
    }

    @Test("Edge: non-finite skipConfidence is returned untouched")
    func nonFiniteSkipConfidencePassThrough() {
        let config = makeConfig(enabled: true)
        let nan = config.applyFragilityPenalty(
            skipConfidence: .nan, proposalConfidence: 0.55,
            promotionTrack: .standard, ledger: fragileLedger()
        )
        #expect(nan.isNaN, "NaN skipConfidence is a data-integrity error; pass it through unchanged")
    }
}
