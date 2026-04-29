// AcousticFingerprintTests.swift
// playhead-gtt9.13: Tests for the compact acoustic fingerprint value type.

import Foundation
import Testing
@testable import Playhead

@Suite("AcousticFingerprint")
struct AcousticFingerprintTests {

    // MARK: - Construction

    @Test("init pads short vectors to fixed length")
    func initPadsShortVectors() {
        let fp = AcousticFingerprint(values: [1.0, 2.0, 3.0])!
        #expect(fp.values.count == AcousticFingerprint.vectorLength)
    }

    @Test("init truncates long vectors to fixed length")
    func initTruncatesLongVectors() {
        let longVec = [Float](repeating: 0.5, count: AcousticFingerprint.vectorLength * 2)
        let fp = AcousticFingerprint(values: longVec)!
        #expect(fp.values.count == AcousticFingerprint.vectorLength)
    }

    @Test("init produces L2-unit-norm vector for non-zero input")
    func initNormalizesToUnit() {
        let fp = AcousticFingerprint(values: [1.0, 2.0, 3.0, 4.0])!
        var sumSq: Float = 0
        for v in fp.values { sumSq += v * v }
        #expect(abs(sumSq - 1.0) < 1e-4)
    }

    @Test("empty input produces canonical zero fingerprint")
    func emptyInputIsZero() {
        let fp = AcousticFingerprint(values: [])!
        #expect(fp.isZero)
    }

    @Test("all-zero input produces canonical zero fingerprint")
    func allZeroInputIsZero() {
        let fp = AcousticFingerprint(values: [Float](repeating: 0, count: 32))!
        #expect(fp.isZero)
    }

    @Test("init rejects vectors with any negative element (fail loud)")
    func rejectsNegativeValues() {
        // Single negative entry: rejected.
        #expect(AcousticFingerprint(values: [1.0, -0.001, 2.0]) == nil)
        // All-negative: rejected.
        #expect(AcousticFingerprint(values: [-1.0, -2.0, -3.0]) == nil)
        // Negative-zero is non-negative under IEEE 754 (-0.0 < 0 is false),
        // so this is a positive control.
        let fp = AcousticFingerprint(values: [Float](repeating: -0.0, count: 4))
        #expect(fp != nil)
        #expect(fp?.isZero == true)
    }

    // MARK: - Similarity properties

    @Test("identity similarity is 1.0")
    func identitySimilarityIsOne() {
        let fp = AcousticFingerprint(values: [1.0, 2.0, 3.0, 4.0, 5.0])!
        let s = AcousticFingerprint.similarity(fp, fp)
        #expect(abs(s - 1.0) < 1e-4)
    }

    @Test("similarity is symmetric")
    func similarityIsSymmetric() {
        let a = AcousticFingerprint(values: [1.0, 2.0, 3.0, 4.0])!
        let b = AcousticFingerprint(values: [4.0, 3.0, 2.0, 1.0])!
        let sab = AcousticFingerprint.similarity(a, b)
        let sba = AcousticFingerprint.similarity(b, a)
        #expect(abs(sab - sba) < 1e-5)
    }

    @Test("similarity is bounded to [0, 1]")
    func similarityIsBounded() {
        // Build several random-ish pairs.
        for seed: Float in stride(from: 0.1, to: 2.0, by: 0.3) {
            let a = AcousticFingerprint(values: (0..<40).map { _ in seed })!
            let b = AcousticFingerprint(values: (0..<40).map { i in Float(i) * seed })!
            let s = AcousticFingerprint.similarity(a, b)
            #expect(s >= 0)
            #expect(s <= 1)
        }
    }

    @Test("orthogonal non-negative fingerprints have similarity ~0")
    func orthogonalSimilarityNearZero() {
        // Construct two vectors with disjoint nonzero positions.
        var va = [Float](repeating: 0, count: AcousticFingerprint.vectorLength)
        var vb = [Float](repeating: 0, count: AcousticFingerprint.vectorLength)
        for i in 0..<32 { va[i] = Float(i + 1) }
        for i in 32..<AcousticFingerprint.vectorLength { vb[i] = Float(i + 1) }

        let a = AcousticFingerprint(values: va)!
        let b = AcousticFingerprint(values: vb)!
        let s = AcousticFingerprint.similarity(a, b)
        #expect(s < 0.01)
    }

    @Test("zero fingerprint never matches")
    func zeroFingerprintNeverMatches() {
        let zero = AcousticFingerprint(values: [])!
        let other = AcousticFingerprint(values: [1.0, 2.0, 3.0])!
        #expect(AcousticFingerprint.similarity(zero, other) == 0)
        #expect(AcousticFingerprint.similarity(zero, zero) == 0)
    }

    @Test("similar but non-identical fingerprints score between 0 and 1")
    func similarVectorsScoreBetween() {
        var va = [Float](repeating: 0, count: 40)
        var vb = [Float](repeating: 0, count: 40)
        for i in 0..<40 {
            va[i] = Float(i + 1)
            vb[i] = Float(i + 1) + Float(i) * 0.1
        }
        let a = AcousticFingerprint(values: va)!
        let b = AcousticFingerprint(values: vb)!
        let s = AcousticFingerprint.similarity(a, b)
        #expect(s > 0.5)
        #expect(s < 1.0)
    }

    // MARK: - Serialization roundtrip

    @Test("data roundtrip preserves fingerprint")
    func dataRoundtripPreservesFingerprint() {
        let original = AcousticFingerprint(values: (0..<64).map { Float($0) * 0.1 })!
        let blob = original.data
        let roundtripped = AcousticFingerprint(data: blob)
        #expect(roundtripped != nil)
        if let rt = roundtripped {
            #expect(AcousticFingerprint.similarity(original, rt) > 0.999)
        }
    }

    @Test("data of wrong length returns nil")
    func dataWrongLengthReturnsNil() {
        let badData = Data([0x01, 0x02, 0x03, 0x04])
        #expect(AcousticFingerprint(data: badData) == nil)
    }

    // MARK: - PCM → fingerprint

    @Test("fromPCM of too-short buffer returns zero")
    func tooShortBufferReturnsZero() {
        let shortPCM: [Float] = Array(repeating: 0.1, count: 10)
        let fp = AcousticFingerprint.fromPCM(shortPCM, sampleRate: 16000)
        #expect(fp.isZero)
    }

    @Test("fromPCM of identical PCM produces identical fingerprints (determinism)")
    func pcmDeterminism() {
        let pcm: [Float] = (0..<8000).map { i in
            sinf(2 * .pi * 440 * Float(i) / 16000)
        }
        let a = AcousticFingerprint.fromPCM(pcm, sampleRate: 16000)
        let b = AcousticFingerprint.fromPCM(pcm, sampleRate: 16000)
        #expect(!a.isZero)
        let s = AcousticFingerprint.similarity(a, b)
        #expect(abs(s - 1.0) < 1e-4)
    }

    @Test("fromPCM completes a 30s 16kHz buffer within budget (perf regression)")
    func pcmFromPcmThirtySecondBudget() {
        // Pre-fix the per-frame DFT was O(windowSize²) ≈ 246M trig calls
        // for 30s @ 16kHz, taking many seconds on a simulator. The
        // vDSP_DFT_zop swap drops this to well under a second on simulator
        // and ~100ms on real device.
        //
        // playhead-rfu-aac (cycle-3 M4): 5s was too loose — a subtler
        // regression (DFT setup recreated per frame instead of reused once)
        // would still complete inside 5s. Tighten to 2s so a recreate-
        // per-frame regression fails the test on simulator while remaining
        // CI-stable. Real device finishes in ~100ms, so the 2s ceiling has
        // ample headroom there.
        let sampleRate: Double = 16_000
        let durationSeconds: Double = 30
        let frameCount = Int(sampleRate * durationSeconds)
        var pcm = [Float]()
        pcm.reserveCapacity(frameCount)
        for i in 0..<frameCount {
            // Mild non-periodic mix so the DFT path actually runs across all bins.
            let t = Float(i) / Float(sampleRate)
            pcm.append(0.3 * sinf(2 * .pi * 220 * t) + 0.2 * sinf(2 * .pi * 1100 * t))
        }
        let start = ContinuousClock.now
        let fp = AcousticFingerprint.fromPCM(pcm, sampleRate: sampleRate)
        let elapsed = ContinuousClock.now - start
        #expect(!fp.isZero)
        #expect(elapsed < .seconds(2), "fromPCM took \(elapsed) — DFT regression suspected (likely setup-recreated-per-frame)")
    }

    @Test("fromPCM distinguishes tones of different frequencies")
    func pcmDistinguishesTones() {
        let tone440: [Float] = (0..<8000).map { i in
            sinf(2 * .pi * 440 * Float(i) / 16000)
        }
        let tone2000: [Float] = (0..<8000).map { i in
            sinf(2 * .pi * 2000 * Float(i) / 16000)
        }
        let a = AcousticFingerprint.fromPCM(tone440, sampleRate: 16000)
        let b = AcousticFingerprint.fromPCM(tone2000, sampleRate: 16000)
        #expect(!a.isZero && !b.isZero)
        let s = AcousticFingerprint.similarity(a, b)
        // Different tones produce distinguishable fingerprints. They
        // won't be orthogonal (the scalars are similar) but should be
        // below the catalog's default 0.80 floor.
        #expect(s < 0.99)
    }
}
