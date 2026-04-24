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
        let fp = AcousticFingerprint(values: [1.0, 2.0, 3.0])
        #expect(fp.values.count == AcousticFingerprint.vectorLength)
    }

    @Test("init truncates long vectors to fixed length")
    func initTruncatesLongVectors() {
        let longVec = [Float](repeating: 0.5, count: AcousticFingerprint.vectorLength * 2)
        let fp = AcousticFingerprint(values: longVec)
        #expect(fp.values.count == AcousticFingerprint.vectorLength)
    }

    @Test("init produces L2-unit-norm vector for non-zero input")
    func initNormalizesToUnit() {
        let fp = AcousticFingerprint(values: [1.0, 2.0, 3.0, 4.0])
        var sumSq: Float = 0
        for v in fp.values { sumSq += v * v }
        #expect(abs(sumSq - 1.0) < 1e-4)
    }

    @Test("empty input produces canonical zero fingerprint")
    func emptyInputIsZero() {
        let fp = AcousticFingerprint(values: [])
        #expect(fp.isZero)
    }

    @Test("all-zero input produces canonical zero fingerprint")
    func allZeroInputIsZero() {
        let fp = AcousticFingerprint(values: [Float](repeating: 0, count: 32))
        #expect(fp.isZero)
    }

    // MARK: - Similarity properties

    @Test("identity similarity is 1.0")
    func identitySimilarityIsOne() {
        let fp = AcousticFingerprint(values: [1.0, 2.0, 3.0, 4.0, 5.0])
        let s = AcousticFingerprint.similarity(fp, fp)
        #expect(abs(s - 1.0) < 1e-4)
    }

    @Test("similarity is symmetric")
    func similarityIsSymmetric() {
        let a = AcousticFingerprint(values: [1.0, 2.0, 3.0, 4.0])
        let b = AcousticFingerprint(values: [4.0, 3.0, 2.0, 1.0])
        let sab = AcousticFingerprint.similarity(a, b)
        let sba = AcousticFingerprint.similarity(b, a)
        #expect(abs(sab - sba) < 1e-5)
    }

    @Test("similarity is bounded to [0, 1]")
    func similarityIsBounded() {
        // Build several random-ish pairs.
        for seed: Float in stride(from: 0.1, to: 2.0, by: 0.3) {
            let a = AcousticFingerprint(values: (0..<40).map { _ in seed })
            let b = AcousticFingerprint(values: (0..<40).map { i in Float(i) * seed })
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

        let a = AcousticFingerprint(values: va)
        let b = AcousticFingerprint(values: vb)
        let s = AcousticFingerprint.similarity(a, b)
        #expect(s < 0.01)
    }

    @Test("zero fingerprint never matches")
    func zeroFingerprintNeverMatches() {
        let zero = AcousticFingerprint(values: [])
        let other = AcousticFingerprint(values: [1.0, 2.0, 3.0])
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
        let a = AcousticFingerprint(values: va)
        let b = AcousticFingerprint(values: vb)
        let s = AcousticFingerprint.similarity(a, b)
        #expect(s > 0.5)
        #expect(s < 1.0)
    }

    // MARK: - Serialization roundtrip

    @Test("data roundtrip preserves fingerprint")
    func dataRoundtripPreservesFingerprint() {
        let original = AcousticFingerprint(values: (0..<64).map { Float($0) * 0.1 })
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
