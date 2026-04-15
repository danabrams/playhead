import Foundation
import Testing

@testable import Playhead

@Suite("AsymmetricSnapScorer")
struct AsymmetricSnapScorerTests {

    // MARK: - Penalty multiplier tests

    @Test("start boundary: too-early gets editorial clip penalty (1.5×)")
    func startTooEarlyClipsEditorial() {
        let mult = AsymmetricSnapScorer.penaltyMultiplier(direction: .adStart, signedError: -2.0)
        #expect(mult == 1.5)
    }

    @Test("start boundary: too-late gets ad leak penalty (1.0×)")
    func startTooLateLeaksAd() {
        let mult = AsymmetricSnapScorer.penaltyMultiplier(direction: .adStart, signedError: 2.0)
        #expect(mult == 1.0)
    }

    @Test("end boundary: too-late gets editorial clip penalty (1.5×)")
    func endTooLateClipsEditorial() {
        let mult = AsymmetricSnapScorer.penaltyMultiplier(direction: .adEnd, signedError: 2.0)
        #expect(mult == 1.5)
    }

    @Test("end boundary: too-early gets ad leak penalty (1.0×)")
    func endTooEarlyLeaksAd() {
        let mult = AsymmetricSnapScorer.penaltyMultiplier(direction: .adEnd, signedError: -2.0)
        #expect(mult == 1.0)
    }

    @Test("zero error always gets baseline penalty")
    func zeroErrorBaseline() {
        #expect(AsymmetricSnapScorer.penaltyMultiplier(direction: .adStart, signedError: 0.0) == 1.0)
        #expect(AsymmetricSnapScorer.penaltyMultiplier(direction: .adEnd, signedError: 0.0) == 1.0)
    }

    // MARK: - Score function tests

    @Test("score applies correct asymmetric penalty for start-too-early")
    func scoreStartTooEarly() {
        let result = AsymmetricSnapScorer.score(
            candidateTime: 10.0,
            snapTarget: 8.0,
            direction: .adStart,
            signedError: -2.0
        )
        // abs(-2.0) * 1.5 = 3.0
        #expect(result == 3.0)
    }

    @Test("score applies baseline penalty for start-too-late")
    func scoreStartTooLate() {
        let result = AsymmetricSnapScorer.score(
            candidateTime: 10.0,
            snapTarget: 12.0,
            direction: .adStart,
            signedError: 2.0
        )
        // abs(2.0) * 1.0 = 2.0
        #expect(result == 2.0)
    }

    @Test("score applies correct asymmetric penalty for end-too-late")
    func scoreEndTooLate() {
        let result = AsymmetricSnapScorer.score(
            candidateTime: 60.0,
            snapTarget: 63.0,
            direction: .adEnd,
            signedError: 3.0
        )
        // abs(3.0) * 1.5 = 4.5
        #expect(result == 4.5)
    }

    @Test("score applies baseline penalty for end-too-early")
    func scoreEndTooEarly() {
        let result = AsymmetricSnapScorer.score(
            candidateTime: 60.0,
            snapTarget: 57.0,
            direction: .adEnd,
            signedError: -3.0
        )
        // abs(-3.0) * 1.0 = 3.0
        #expect(result == 3.0)
    }

    // MARK: - Signal tier classification tests

    @Test("strong tier: bracket score above 0.7")
    func strongTierHighBracket() {
        let tier = AsymmetricSnapScorer.signalTier(
            bracketScore: 0.85,
            boundaryCues: [:]
        )
        #expect(tier == .strong)
    }

    @Test("strong tier: silence gap present")
    func strongTierSilenceGap() {
        let tier = AsymmetricSnapScorer.signalTier(
            bracketScore: nil,
            boundaryCues: [.silenceGap: 0.8]
        )
        #expect(tier == .strong)
    }

    @Test("moderate tier: bracket score 0.4–0.7")
    func moderateTierMidBracket() {
        let tier = AsymmetricSnapScorer.signalTier(
            bracketScore: 0.55,
            boundaryCues: [:]
        )
        #expect(tier == .moderate)
    }

    @Test("moderate tier: spectral cue present")
    func moderateTierSpectral() {
        let tier = AsymmetricSnapScorer.signalTier(
            bracketScore: nil,
            boundaryCues: [.spectralDiscontinuity: 0.6]
        )
        #expect(tier == .moderate)
    }

    @Test("weak tier: no bracket, no strong cues")
    func weakTierNoCues() {
        let tier = AsymmetricSnapScorer.signalTier(
            bracketScore: nil,
            boundaryCues: [:]
        )
        #expect(tier == .weak)
    }

    @Test("weak tier: low bracket score below 0.4")
    func weakTierLowBracket() {
        let tier = AsymmetricSnapScorer.signalTier(
            bracketScore: 0.2,
            boundaryCues: [:]
        )
        #expect(tier == .weak)
    }

    @Test("weak tier: low-confidence cues below threshold")
    func weakTierLowConfidenceCues() {
        let tier = AsymmetricSnapScorer.signalTier(
            bracketScore: nil,
            boundaryCues: [.silenceGap: 0.1, .spectralDiscontinuity: 0.2]
        )
        #expect(tier == .weak)
    }

    // MARK: - Dynamic snap radius tests

    @Test("strong cues produce radius in 3–6s range")
    func dynamicRadiusStrong() {
        let radius = AsymmetricSnapScorer.dynamicSnapRadius(
            bracketScore: 0.9,
            boundaryCues: [:],
            priorSpread: nil
        )
        #expect(radius >= 3.0 && radius <= 6.0)
    }

    @Test("moderate cues produce radius in 6–8s range")
    func dynamicRadiusModerate() {
        let radius = AsymmetricSnapScorer.dynamicSnapRadius(
            bracketScore: 0.5,
            boundaryCues: [:],
            priorSpread: nil
        )
        #expect(radius >= 6.0 && radius <= 8.0)
    }

    @Test("weak cues produce radius up to 10s")
    func dynamicRadiusWeak() {
        let radius = AsymmetricSnapScorer.dynamicSnapRadius(
            bracketScore: nil,
            boundaryCues: [:],
            priorSpread: nil
        )
        #expect(radius <= 10.0 && radius >= 6.0)
    }

    @Test("prior spread blends with tier base radius")
    func dynamicRadiusWithPriorSpread() {
        // Strong tier base = 6.0, prior spread = 4.0
        // blended = 0.6 * 6.0 + 0.4 * 4.0 = 3.6 + 1.6 = 5.2
        let radius = AsymmetricSnapScorer.dynamicSnapRadius(
            bracketScore: 0.9,
            boundaryCues: [:],
            priorSpread: 4.0
        )
        #expect(abs(radius - 5.2) < 0.01)
    }

    @Test("prior spread is clamped to valid range")
    func dynamicRadiusPriorSpreadClamped() {
        // Weak tier base = 10.0, prior spread = 20.0
        // blended = 0.6 * 10.0 + 0.4 * 20.0 = 6.0 + 8.0 = 14.0 → clamped to 10.0
        let radius = AsymmetricSnapScorer.dynamicSnapRadius(
            bracketScore: nil,
            boundaryCues: [:],
            priorSpread: 20.0
        )
        #expect(radius == 10.0)
    }

    @Test("very small prior spread clamps to minimum")
    func dynamicRadiusSmallPriorSpread() {
        // Strong tier base = 6.0, prior spread = 0.5
        // blended = 0.6 * 6.0 + 0.4 * 0.5 = 3.6 + 0.2 = 3.8 → within [3, 10]
        let radius = AsymmetricSnapScorer.dynamicSnapRadius(
            bracketScore: 0.9,
            boundaryCues: [:],
            priorSpread: 0.5
        )
        #expect(radius >= 3.0 && radius <= 10.0)
    }

    // MARK: - SignedBoundaryError tests

    @Test("buildError computes correct signed error and penalty")
    func buildErrorStartTooEarly() {
        let error = AsymmetricSnapScorer.buildError(
            spanId: "test-span-1",
            direction: .adStart,
            snapTarget: 8.0,
            trueTime: 10.0
        )
        #expect(error.spanId == "test-span-1")
        #expect(error.direction == .adStart)
        #expect(error.signedErrorSeconds == -2.0) // 8 - 10 = -2
        #expect(error.penaltyMultiplier == 1.5)   // start + negative = editorial clip
        #expect(error.penalizedError == 3.0)       // abs(-2) * 1.5
    }

    @Test("buildError end boundary too-late")
    func buildErrorEndTooLate() {
        let error = AsymmetricSnapScorer.buildError(
            spanId: "test-span-2",
            direction: .adEnd,
            snapTarget: 65.0,
            trueTime: 60.0
        )
        #expect(error.signedErrorSeconds == 5.0)   // 65 - 60 = 5
        #expect(error.penaltyMultiplier == 1.5)    // end + positive = editorial clip
        #expect(error.penalizedError == 7.5)        // abs(5) * 1.5
    }

    @Test("buildError ad leak cases get 1.0× multiplier")
    func buildErrorAdLeak() {
        let startLate = AsymmetricSnapScorer.buildError(
            spanId: "s", direction: .adStart, snapTarget: 12.0, trueTime: 10.0
        )
        #expect(startLate.penaltyMultiplier == 1.0) // start + positive = ad leak

        let endEarly = AsymmetricSnapScorer.buildError(
            spanId: "s", direction: .adEnd, snapTarget: 58.0, trueTime: 60.0
        )
        #expect(endEarly.penaltyMultiplier == 1.0) // end + negative = ad leak
    }

    @Test("aggregate penalized error computes mean")
    func aggregateError() {
        let errors = [
            SignedBoundaryError(spanId: "a", direction: .adStart, signedErrorSeconds: -2.0, penaltyMultiplier: 1.5),
            SignedBoundaryError(spanId: "b", direction: .adEnd, signedErrorSeconds: 4.0, penaltyMultiplier: 1.5),
        ]
        // penalized: 3.0 + 6.0 = 9.0, mean = 4.5
        let agg = AsymmetricSnapScorer.aggregatePenalizedError(errors)
        #expect(abs(agg - 4.5) < 0.001)
    }

    @Test("aggregate penalized error returns 0 for empty array")
    func aggregateErrorEmpty() {
        #expect(AsymmetricSnapScorer.aggregatePenalizedError([]) == 0)
    }

    // MARK: - Asymmetry bias verification

    @Test("asymmetry always prefers leaking ad over clipping editorial")
    func asymmetryBiasVerification() {
        // Same magnitude error, different directions
        let clipEditorial = AsymmetricSnapScorer.score(
            candidateTime: 10, snapTarget: 8, direction: .adStart, signedError: -2.0
        )
        let leakAd = AsymmetricSnapScorer.score(
            candidateTime: 10, snapTarget: 12, direction: .adStart, signedError: 2.0
        )
        // Clipping editorial should score worse (higher penalty)
        #expect(clipEditorial > leakAd)
    }
}
