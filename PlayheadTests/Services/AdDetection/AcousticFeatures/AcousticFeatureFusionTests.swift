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

    @Test("default weights sum to ≈1.0 within epsilon (M5)")
    func defaultWeightsSumToOne() {
        let w = AcousticFeatureFusion.Weights.defaultPriors
        let sum = w.musicBed + w.lufsShift + w.dynamicRange + w.speakerShift +
            w.spectralShift + w.silenceBoundary + w.repetitionFingerprint + w.tempoOnset
        #expect(abs(sum - 1.0) < 1e-9)
    }
}
