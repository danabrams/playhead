// EVIScorerTests.swift
// Tests for EVI scoring heuristics and priority reason coverage.

import Testing

@testable import Playhead

@Suite("EVIScorer")
struct EVIScorerTests {

    // MARK: - flipProbability

    @Test("flipProbability is 1.0 at confidence 0.5")
    func testFlipProbabilityMaxAtHalf() {
        let p = EVIScorer.flipProbability(currentConfidence: 0.5)
        #expect(p == 1.0)
    }

    @Test("flipProbability is 0.0 at confidence 0.0 and 1.0")
    func testFlipProbabilityZeroAtExtremes() {
        #expect(EVIScorer.flipProbability(currentConfidence: 0.0) == 0.0)
        #expect(EVIScorer.flipProbability(currentConfidence: 1.0) == 0.0)
    }

    @Test("flipProbability symmetric around 0.5")
    func testFlipProbabilitySymmetric() {
        let low = EVIScorer.flipProbability(currentConfidence: 0.3)
        let high = EVIScorer.flipProbability(currentConfidence: 0.7)
        #expect(abs(low - high) < 0.001)
    }

    @Test("flipProbability clamps out-of-range inputs")
    func testFlipProbabilityClamps() {
        #expect(EVIScorer.flipProbability(currentConfidence: -0.5) == 0.0)
        #expect(EVIScorer.flipProbability(currentConfidence: 1.5) == 0.0)
    }

    @Test("flipProbability increases toward 0.5 from both sides")
    func testFlipProbabilityMonotonicity() {
        let at02 = EVIScorer.flipProbability(currentConfidence: 0.2)
        let at03 = EVIScorer.flipProbability(currentConfidence: 0.3)
        let at04 = EVIScorer.flipProbability(currentConfidence: 0.4)
        #expect(at02 < at03)
        #expect(at03 < at04)

        let at08 = EVIScorer.flipProbability(currentConfidence: 0.8)
        let at07 = EVIScorer.flipProbability(currentConfidence: 0.7)
        let at06 = EVIScorer.flipProbability(currentConfidence: 0.6)
        #expect(at08 < at07)
        #expect(at07 < at06)
    }

    // MARK: - EVIScore computed property

    @Test("score = flipProb * utility / max(cost, 0.01)")
    func testScoreFormula() {
        let s = EVIScore(
            decisionFlipProbability: 0.8,
            utilityGain: 1.0,
            computeCost: 0.4,
            reason: nil
        )
        let expected: Float = 0.8 * 1.0 / 0.4
        #expect(abs(s.score - expected) < 0.001)
    }

    @Test("score with zero cost uses floor of 0.01")
    func testScoreZeroCostFloor() {
        let s = EVIScore(
            decisionFlipProbability: 0.5,
            utilityGain: 1.0,
            computeCost: 0.0,
            reason: nil
        )
        let expected: Float = 0.5 / 0.01
        #expect(abs(s.score - expected) < 0.001)
    }

    @Test("score is zero when flipProbability is zero")
    func testScoreZeroFlipProb() {
        let s = EVIScore(
            decisionFlipProbability: 0.0,
            utilityGain: 1.0,
            computeCost: 0.5,
            reason: nil
        )
        #expect(s.score == 0.0)
    }

    // MARK: - EVIScorer.score

    @Test("score clamps computeCost to [0, 1]")
    func testScoreClampsCost() {
        let s = EVIScorer.score(
            currentConfidence: 0.5,
            computeCost: 1.5,
            reason: nil
        )
        #expect(s.computeCost == 1.0)

        let s2 = EVIScorer.score(
            currentConfidence: 0.5,
            computeCost: -0.5,
            reason: nil
        )
        #expect(s2.computeCost == 0.0)
    }

    @Test("score passes reason through")
    func testScorePassesReason() {
        let s = EVIScorer.score(
            currentConfidence: 0.5,
            computeCost: 0.1,
            reason: .coldStartShow
        )
        #expect(s.reason == .coldStartShow)
    }

    // MARK: - EVIScorer.rank

    @Test("rank returns highest EVI first")
    func testRankOrder() {
        let candidates: [(confidence: Float, cost: Float, reason: EVIPriorityReason?)] = [
            (confidence: 0.9, cost: 0.5, reason: nil),      // low flip prob
            (confidence: 0.5, cost: 0.5, reason: nil),      // high flip prob
            (confidence: 0.5, cost: 0.1, reason: nil),      // high flip prob, low cost
        ]
        let ranked = EVIScorer.rank(candidates)
        #expect(ranked.count == 3)
        // Lowest cost at 0.5 confidence should rank first.
        #expect(ranked[0].computeCost == 0.1)
        #expect(ranked[0].score > ranked[1].score)
        #expect(ranked[1].score > ranked[2].score)
    }

    @Test("rank handles empty input")
    func testRankEmpty() {
        let ranked = EVIScorer.rank([])
        #expect(ranked.isEmpty)
    }

    // MARK: - EVIPriorityReason

    @Test("all 7 priority reasons exist")
    func testAllReasonsPresent() {
        #expect(EVIPriorityReason.allCases.count == 7)
    }

    @Test("priority reasons round-trip through raw value")
    func testReasonRawValueRoundTrip() {
        for reason in EVIPriorityReason.allCases {
            let rebuilt = EVIPriorityReason(rawValue: reason.rawValue)
            #expect(rebuilt == reason)
        }
    }

    @Test("EVIScore clamps out-of-range fields at init")
    func testEVIScoreClamping() {
        let s = EVIScore(
            decisionFlipProbability: 1.5,
            utilityGain: -2.0,
            computeCost: 3.0,
            reason: nil
        )
        #expect(s.decisionFlipProbability == 1.0)
        #expect(s.utilityGain == 0.0)
        #expect(s.computeCost == 1.0)
    }
}
