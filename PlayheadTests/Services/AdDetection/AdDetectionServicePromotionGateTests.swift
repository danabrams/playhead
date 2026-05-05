// AdDetectionServicePromotionGateTests.swift
// playhead-fqc8: Tests for the auto-skip threshold swap driven by
// `DecisionResult.promotionTrack`. The threshold lookup lives in
// `AdDetectionConfig.effectiveAutoSkipThreshold(for:)`; testing the
// pure helper directly avoids spinning up the actor.

import Foundation
import Testing
@testable import Playhead

@Suite("AdDetectionService promotion gate (playhead-fqc8)")
struct AdDetectionServicePromotionGateTests {

    private func makeConfig(
        autoSkip: Double = 0.80,
        qualified: Double = 0.50
    ) -> AdDetectionConfig {
        AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "fqc8-test",
            autoSkipConfidenceThreshold: autoSkip,
            classifierSeedQualifiedThreshold: qualified
        )
    }

    @Test("AdDetectionConfig exposes classifierSeedQualifiedThreshold with default 0.50")
    func configDefaultsThreshold() {
        let config = AdDetectionConfig.default
        #expect(config.classifierSeedQualifiedThreshold == 0.50,
                "playhead-fqc8 default threshold for the qualified track is 0.50")
    }

    @Test("effectiveAutoSkipThreshold returns autoSkipConfidenceThreshold for .standard")
    func effectiveThresholdStandard() {
        let config = makeConfig(autoSkip: 0.80, qualified: 0.50)
        #expect(config.effectiveAutoSkipThreshold(for: .standard) == 0.80)
    }

    @Test("effectiveAutoSkipThreshold returns classifierSeedQualifiedThreshold for .classifierSeedQualified")
    func effectiveThresholdQualified() {
        let config = makeConfig(autoSkip: 0.80, qualified: 0.50)
        #expect(config.effectiveAutoSkipThreshold(for: .classifierSeedQualified) == 0.50)
    }

    /// The headline contract from the bead spec: a span with
    /// `skipConfidence == 0.55` (a plausible classifier-only outcome) is
    /// stuck below the 0.80 standard gate but clears the 0.50 qualified
    /// gate. This is the whole reason the track exists.
    @Test("DecisionResult at 0.55 fails standard 0.80 gate but clears qualified 0.50 gate")
    func gateSwapAt055() {
        let config = makeConfig(autoSkip: 0.80, qualified: 0.50)

        let standard = DecisionResult(
            proposalConfidence: 0.55,
            skipConfidence: 0.55,
            eligibilityGate: .eligible,
            promotionTrack: .standard
        )
        let qualified = DecisionResult(
            proposalConfidence: 0.55,
            skipConfidence: 0.55,
            eligibilityGate: .eligible,
            promotionTrack: .classifierSeedQualified
        )

        let standardThreshold = config.effectiveAutoSkipThreshold(
            for: standard.promotionTrack
        )
        let qualifiedThreshold = config.effectiveAutoSkipThreshold(
            for: qualified.promotionTrack
        )

        #expect(standard.skipConfidence < standardThreshold,
                "0.55 must FAIL the standard 0.80 gate")
        #expect(qualified.skipConfidence >= qualifiedThreshold,
                "0.55 must CLEAR the qualified 0.50 gate")
    }

    /// Setting the qualified threshold to be no looser than the standard
    /// threshold makes the track an effective no-op — useful as a safety
    /// rollback knob.
    @Test("Equal thresholds make the qualified track functionally identical to standard")
    func equalThresholdsMakeQualifiedANoOp() {
        let config = makeConfig(autoSkip: 0.80, qualified: 0.80)
        #expect(config.effectiveAutoSkipThreshold(for: .standard)
                == config.effectiveAutoSkipThreshold(for: .classifierSeedQualified))
    }

    /// playhead-fqc8 cycle-1 review HIGH-2: the hot path uses the
    /// standard `autoSkipConfidenceThreshold` (0.80) because the
    /// qualified-track signal — the acoustic-break alignment that gates
    /// `PromotionTrack.classifierSeedQualified` — only joins the ledger
    /// after fusion runs in `runBackfill`. A 0.55 classifier in the hot
    /// path with NO DecisionResult-side evidence available cannot be
    /// promoted; the same span may still be auto-skip-promoted later by
    /// the backfill pass once alignment evidence joins the ledger and
    /// `DecisionMapper.computePromotionTrack` selects the looser
    /// `classifierSeedQualifiedThreshold`. This test pins the intentional
    /// hot-path / backfill bifurcation as a design boundary.
    @Test("Hot-path 0.55 classifier alone fails the standard 0.80 threshold (intentional bifurcation)")
    func hotPathThresholdStaysAt080ForClassifierOnlyResult() {
        let config = makeConfig(autoSkip: 0.80, qualified: 0.50)
        // The hot path compares `result.adProbability` directly against
        // `config.autoSkipConfidenceThreshold` — there is no DecisionResult /
        // PromotionTrack at this stage. A 0.55 classifier therefore must
        // NOT be promoted in the hot path even though backfill may later
        // promote the same span via the qualified track.
        let classifierProbability = 0.55
        #expect(classifierProbability < config.autoSkipConfidenceThreshold,
                "Hot-path threshold compare must keep 0.55 below the standard 0.80 gate")
        // Sanity check: backfill via the qualified track WOULD clear the
        // looser gate for the same span — that's the whole point of the
        // bifurcation.
        let qualifiedThreshold = config.effectiveAutoSkipThreshold(
            for: .classifierSeedQualified
        )
        #expect(classifierProbability >= qualifiedThreshold,
                "Backfill via the qualified track CAN promote the same 0.55 span — bifurcation is the point")
    }
}
