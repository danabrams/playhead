// RuleBasedClassifierMusicRetuneTests.swift
// playhead-uxte: pin the retuned computeMusicScore gain (1.5 -> 3.0) and the
// restored RuleBasedClassifier operating point after playhead-riiz (#231)
// un-saturated acousticMusicProbability.
//
// Background (measured on the 34-episode corpus at the pipeline's 2 s
// windows; see the playhead-uxte bead for the measurement):
//   - Pre-riiz, speech windows carried musicProbability ~0.725 (the OLD
//     formula saturated), so min(avg * 1.5, 1) pinned musicScore at 1.0 for
//     94% of speech candidates — a constant +0.10-weighted rawScore bias.
//   - Post-riiz, speech windows carry ~0.434, so under gain 1.5 the term
//     fell to ~0.65: a -0.034 rawScore / -0.065 calibrated drop that pushed
//     borderline host-read ads below the orchestrator's 0.65 enter
//     threshold (SkipOrchestrator.Config.enterThreshold — NOT changed by
//     the retune; the classifier side was retuned instead).
//   - Gain 3.0 restores the speech saturation (knee at avgMusic 1/3, the
//     ~7th percentile of speech candidates, so ~93% saturate at 1.0 as
//     before) while the avgMusic > 0.8 playout cap branch is untouched, so
//     music segments are not boosted.
//
// The three-state operating-point test below synthesizes the same borderline
// host-read-shaped candidate in all three states:
//   1. pre-riiz     (musicProbability 0.72)      -> eligible (>= 0.65)
//   2. post-riiz + old gain 1.5 (counterfactual) -> fell below 0.65
//   3. post-riiz + retuned gain 3.0 (shipped)    -> restored (>= 0.65)
// State 2 is reconstructed arithmetically from the shipped classifier's own
// SignalBreakdown plus the published weights/sigmoid constants; a separate
// assertion proves that same arithmetic reproduces the live adProbability
// exactly, so the counterfactual cannot silently drift from the real code.

import Foundation
import Testing
@testable import Playhead

@Suite("RuleBasedClassifier music retune (playhead-uxte)")
struct RuleBasedClassifierMusicRetuneTests {

    // MARK: - Constants mirrored from RuleBasedClassifier (validated below)

    /// Signal weights (RuleBasedClassifier.Weight). `constantsMirrorLiveCode`
    /// proves these reproduce the live adProbability bit-for-bit.
    private enum MirroredWeight {
        static let lexical = 0.40
        static let rmsDrop = 0.20
        static let spectralChange = 0.15
        static let music = 0.10
        static let speakerChange = 0.05
        static let prior = 0.10
    }
    private let mirroredSigmoidK = 8.0
    private let mirroredSigmoidMid = 0.25

    /// SkipPolicyConfig.default.enterThreshold — the consumer of
    /// adProbability this retune restores the operating point against.
    private let enterThreshold = SkipPolicyConfig.default.enterThreshold

    // Empirical anchors (34-episode corpus, 2 s pipeline windows):
    private let preRiizSpeechMusicProb = 0.72   // OLD formula speech mean 0.725
    private let postRiizSpeechMusicProb = 0.44  // NEW composite speech mean 0.434
    private let postRiizPlayoutMusicProb = 0.91 // NEW composite playout mean 0.906

    /// Lexical confidence placing the pre-riiz candidate just above the
    /// enter threshold: raw = 0.40*0.57 + 0.10*1.0 = 0.328 -> adProb 0.6511.
    private let borderlineLexicalConfidence = 0.57

    // MARK: - Fixtures

    /// Windows that isolate the music term: constant rms (no rmsDrop score),
    /// constant flux (no spectralChange score), nil speaker clusters (no
    /// speakerChange score). With `.empty` priors, rawScore reduces to
    /// 0.40 * lexical + 0.10 * musicScore.
    private func speechShapedWindows(
        musicProbability: Double,
        count: Int = 15
    ) -> [FeatureWindow] {
        (0..<count).map { index in
            FeatureWindow(
                analysisAssetId: "asset-uxte",
                startTime: Double(index) * 2.0,
                endTime: Double(index) * 2.0 + 2.0,
                rms: 0.05,
                spectralFlux: 0.10,
                musicProbability: musicProbability,
                pauseProbability: 0.2,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 5
            )
        }
    }

    private func hostReadCandidate(confidence: Double) -> LexicalCandidate {
        LexicalCandidate(
            id: "cand-uxte",
            analysisAssetId: "asset-uxte",
            startTime: 0.0,
            endTime: 30.0,
            confidence: confidence,
            hitCount: 2,
            categories: [.sponsor],
            evidenceText: "today's episode is brought to you by",
            evidenceStartTime: 1.0,
            detectorVersion: "test"
        )
    }

    private func classify(musicProbability: Double) -> ClassifierResult {
        RuleBasedClassifier().classify(
            input: ClassifierInput(
                candidate: hostReadCandidate(confidence: borderlineLexicalConfidence),
                featureWindows: speechShapedWindows(musicProbability: musicProbability),
                episodeDuration: 3600.0
            ),
            priors: .empty
        )
    }

    private func mirroredAdProbability(
        breakdown: SignalBreakdown,
        musicScore: Double
    ) -> Double {
        let raw =
            MirroredWeight.lexical * breakdown.lexicalScore +
            MirroredWeight.rmsDrop * breakdown.rmsDropScore +
            MirroredWeight.spectralChange * breakdown.spectralChangeScore +
            MirroredWeight.music * musicScore +
            MirroredWeight.speakerChange * breakdown.speakerChangeScore +
            MirroredWeight.prior * breakdown.priorScore
        return 1.0 / (1.0 + exp(-mirroredSigmoidK * (raw - mirroredSigmoidMid)))
    }

    // MARK: - computeMusicScore mapping pins

    @Test("gain is 3.0: post-riiz speech saturates, knee sits at avgMusic 1/3")
    func gainPins() {
        // Post-riiz speech mean (0.44) saturates at 1.0 — the restored bias.
        #expect(RegionScoring.computeMusicScore(
            windows: speechShapedWindows(musicProbability: postRiizSpeechMusicProb)) == 1.0)
        // Speech p5 (0.313) lands just under the knee but within 0.061 of
        // saturation — the residual the measurement bounded at p5 -0.0099
        // calibrated.
        expectNearlyEqual(
            RegionScoring.computeMusicScore(
                windows: speechShapedWindows(musicProbability: 0.313)),
            0.939
        )
        // Two below-knee points pin the gain constant numerically: a gain
        // other than 3.0 fails one of these exactly.
        expectNearlyEqual(
            RegionScoring.computeMusicScore(
                windows: speechShapedWindows(musicProbability: 0.30)),
            0.90
        )
        expectNearlyEqual(
            RegionScoring.computeMusicScore(
                windows: speechShapedWindows(musicProbability: 0.20)),
            0.60
        )
    }

    @Test("playout cap branch is untouched by the retune")
    func playoutCapUnchanged() {
        // Post-riiz playout mean (0.906) takes the 0.5 cap.
        #expect(RegionScoring.computeMusicScore(
            windows: speechShapedWindows(musicProbability: postRiizPlayoutMusicProb)) == 0.5)
        // Cap boundary is strict `> 0.8` (pre-existing semantics). Single
        // windows keep the average exact: 15 accumulated 0.8s average to
        // 0.8000000000000002 and would falsely take the cap branch.
        #expect(RegionScoring.computeMusicScore(
            windows: speechShapedWindows(musicProbability: 0.8, count: 1)) == 1.0)
        #expect(RegionScoring.computeMusicScore(
            windows: speechShapedWindows(musicProbability: 0.81, count: 1)) == 0.5)
        // Empty guard unchanged.
        #expect(RegionScoring.computeMusicScore(windows: []) == 0.0)
    }

    // MARK: - Operating point: pre-riiz / post-riiz-unfixed / retuned

    @Test("mirrored constants reproduce the live adProbability exactly")
    func constantsMirrorLiveCode() {
        // If RuleBasedClassifier's weights or sigmoid constants drift, the
        // counterfactual arithmetic below is invalid — this assertion makes
        // that drift loud.
        for musicProbability in [preRiizSpeechMusicProb, postRiizSpeechMusicProb, postRiizPlayoutMusicProb] {
            let result = classify(musicProbability: musicProbability)
            let mirrored = mirroredAdProbability(
                breakdown: result.signalBreakdown,
                musicScore: result.signalBreakdown.musicScore
            )
            #expect(abs(result.adProbability - mirrored) < 1e-12)
        }
    }

    @Test("borderline host-read: eligible pre-riiz, fell under gain 1.5, restored by gain 3.0")
    func borderlineHostReadRestored() {
        // State 1 — pre-riiz: speech windows at the OLD-formula level (0.72).
        // Both gains saturate 0.72 to musicScore 1.0, so the shipped
        // classifier reproduces the pre-riiz operating point faithfully.
        let preRiiz = classify(musicProbability: preRiizSpeechMusicProb)
        #expect(preRiiz.signalBreakdown.musicScore == 1.0)
        #expect(preRiiz.adProbability >= enterThreshold)
        expectNearlyEqual(preRiiz.adProbability, 0.6511, tolerance: 0.001)

        // State 3 — post-riiz + retune (the shipped code): speech windows at
        // the NEW-composite level (0.44) score identically to pre-riiz.
        let retuned = classify(musicProbability: postRiizSpeechMusicProb)
        #expect(retuned.signalBreakdown.musicScore == 1.0)
        #expect(retuned.adProbability >= enterThreshold)
        #expect(abs(retuned.adProbability - preRiiz.adProbability) < 1e-12)

        // State 2 — post-riiz + OLD gain 1.5 (the regression this bead
        // fixes), reconstructed from the shipped classifier's breakdown via
        // the mirrored constants validated above.
        let oldGainMusicScore = min(postRiizSpeechMusicProb * 1.5, 1.0)
        let unfixed = mirroredAdProbability(
            breakdown: retuned.signalBreakdown,
            musicScore: oldGainMusicScore
        )
        #expect(unfixed < enterThreshold)
        expectNearlyEqual(unfixed, 0.5871, tolerance: 0.001)
    }

    @Test("music playout does not newly cross the enter threshold")
    func playoutNotBoosted() {
        // Same borderline candidate over playout-level music (0.91): the cap
        // branch yields 0.5 and the adProbability sits well below enter.
        let playout = classify(musicProbability: postRiizPlayoutMusicProb)
        #expect(playout.signalBreakdown.musicScore == 0.5)
        #expect(playout.adProbability < enterThreshold)
        expectNearlyEqual(playout.adProbability, 0.5558, tolerance: 0.001)

        // The retune is a no-op on the cap branch: gain 1.5 and gain 3.0
        // produce the identical adProbability for playout candidates.
        let unretuned = mirroredAdProbability(
            breakdown: playout.signalBreakdown,
            musicScore: 0.5 // cap branch is gain-independent
        )
        #expect(abs(playout.adProbability - unretuned) < 1e-12)

        // And playout under the retune never exceeds its pre-riiz score:
        // pre-riiz acoustic playout averaged 0.737 (below the cap) and
        // scored musicScore 1.0 -> a HIGHER adProbability than today's 0.5
        // cap. The retune only ever lowers playout relative to pre-riiz.
        let preRiizPlayout = classify(musicProbability: 0.737)
        #expect(preRiizPlayout.signalBreakdown.musicScore == 1.0)
        #expect(playout.adProbability < preRiizPlayout.adProbability)
    }
}

// MARK: - Helpers

private func expectNearlyEqual(
    _ actual: Double,
    _ expected: Double,
    tolerance: Double = 1e-9,
    sourceLocation: SourceLocation = #_sourceLocation
) {
    #expect(
        abs(actual - expected) <= tolerance,
        "expected \(expected) ± \(tolerance), got \(actual)",
        sourceLocation: sourceLocation
    )
}
