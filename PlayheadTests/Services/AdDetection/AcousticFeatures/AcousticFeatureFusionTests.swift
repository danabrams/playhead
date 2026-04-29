// AcousticFeatureFusionTests.swift
// playhead-rfu-aac: pin determinism + weight-sum invariants for the
// per-window fusion combiner.

import Foundation
import Testing

@testable import Playhead

@Suite("AcousticFeatureFusion")
struct AcousticFeatureFusionTests {

    private func sampleScores(
        kind: AcousticFeatureKind,
        score: Double,
        count: Int
    ) -> [AcousticFeatureScore] {
        (0..<count).map { i in
            AcousticFeatureScore(
                feature: kind,
                windowStart: Double(i) * 2,
                windowEnd: Double(i + 1) * 2,
                score: score,
                rawMetric: score
            )
        }
    }

    @Test("combine is deterministic across repeated calls (M4)")
    func combineDeterministic() {
        let inputs: [AcousticFeatureKind: [AcousticFeatureScore]] = [
            .musicBed: sampleScores(kind: .musicBed, score: 0.7, count: 5),
            .lufsShift: sampleScores(kind: .lufsShift, score: 0.5, count: 5),
            .silenceBoundary: sampleScores(kind: .silenceBoundary, score: 0.3, count: 5),
            .speakerShift: sampleScores(kind: .speakerShift, score: 0.6, count: 5),
            .dynamicRange: sampleScores(kind: .dynamicRange, score: 0.4, count: 5)
        ]
        let a = AcousticFeatureFusion.combine(featureScores: inputs)
        let b = AcousticFeatureFusion.combine(featureScores: inputs)
        let c = AcousticFeatureFusion.combine(featureScores: inputs)
        #expect(a == b)
        #expect(b == c)
        // Window bounds also stable.
        #expect(a.map(\.windowStart) == b.map(\.windowStart))
    }

    @Test("combine tiebreak is alphabetical-by-rawValue, not hash-seed dependent (cycle-3 M1)")
    func combineDeterministicTiebreak() {
        // Construct a fusion input where every contributing feature gates
        // through with the same score. Within a single window, the
        // `contributingFeatures` list MUST come out sorted by enum rawValue
        // (== alphabetical for a String-raw enum) — that's the deterministic
        // tiebreak the production combiner promises.
        //
        // Same-process equality (a == b == c above) holds even with a
        // hash-seed-dependent ordering because Swift fixes the per-process
        // hash seed. To pin true determinism, hard-code the expected first-
        // element ordering against the rawValue ASC contract.
        let inputs: [AcousticFeatureKind: [AcousticFeatureScore]] = [
            .musicBed: sampleScores(kind: .musicBed, score: 0.5, count: 1),
            .lufsShift: sampleScores(kind: .lufsShift, score: 0.5, count: 1),
            .silenceBoundary: sampleScores(kind: .silenceBoundary, score: 0.5, count: 1),
            .speakerShift: sampleScores(kind: .speakerShift, score: 0.5, count: 1),
            .dynamicRange: sampleScores(kind: .dynamicRange, score: 0.5, count: 1)
        ]
        let result = AcousticFeatureFusion.combine(featureScores: inputs)
        #expect(result.count == 1)
        let contributing = result.first?.contributingFeatures ?? []
        // Hand-computed expected ordering: rawValue (== case name for a
        // String enum) ASC. dynamicRange < lufsShift < musicBed <
        // silenceBoundary < speakerShift.
        let expected: [AcousticFeatureKind] = [
            .dynamicRange,
            .lufsShift,
            .musicBed,
            .silenceBoundary,
            .speakerShift
        ]
        #expect(contributing == expected)

        // Window bounds come from `referencePriority` (musicBed first), so
        // the first windowStart is sourced from the musicBed score whose
        // sampleScores() puts windowStart at idx*2 == 0 for idx 0.
        #expect(result.first?.windowStart == 0.0)
    }

    @Test("default weights sum to ≈1.0 within epsilon (M5)")
    func defaultWeightsSumToOne() {
        let w = AcousticFeatureFusion.Weights.defaultPriors
        let sum = w.musicBed + w.lufsShift + w.dynamicRange + w.speakerShift +
            w.spectralShift + w.silenceBoundary + w.repetitionFingerprint + w.tempoOnset
        #expect(abs(sum - 1.0) < 1e-9)
    }

    @Test("AcousticFeatureScore NaN input clamps to zero (cycle-3 L6)")
    func nanScoreClampsToZero() {
        // A NaN upstream metric (e.g. divide-by-zero in a feature
        // implementation) MUST NOT poison fusion arithmetic. The init
        // delegates to `clampUnit`, which converts NaN → 0.
        let nanScore = AcousticFeatureScore(
            feature: .musicBed,
            windowStart: 0,
            windowEnd: 2,
            score: .nan,
            rawMetric: 0
        )
        #expect(nanScore.score == 0)

        // Sanity: the same path also clamps out-of-range finite inputs.
        let overflow = AcousticFeatureScore(
            feature: .musicBed,
            windowStart: 0,
            windowEnd: 2,
            score: 1.5,
            rawMetric: 0
        )
        #expect(overflow.score == 1.0)
        let underflow = AcousticFeatureScore(
            feature: .musicBed,
            windowStart: 0,
            windowEnd: 2,
            score: -0.5,
            rawMetric: 0
        )
        #expect(underflow.score == 0.0)
    }
}
