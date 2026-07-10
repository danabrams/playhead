// ChromaFingerprinterTests.swift
// playhead-xsdz.26: unit tests for every pure stage of the clean-room
// chromaprint-class fingerprinter — windowing, FFT wrapper, chroma folding,
// chroma-image normalization, filter responses, quantization/Gray-coding,
// packing, stream assembly, determinism, and the granularity pin the rediff
// validation suite relies on.
//
// Hermetic: synthetic PCM only (sinusoids/silence). Corpus-backed validation
// lives in ChromaFingerprinterRediffValidationTests.

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private let sampleRate = Double(ChromaFingerprinter.requiredSampleRate)

/// A sinusoid of `count` samples at `frequency` Hz (11025 Hz timeline).
private func sinusoid(frequency: Double, count: Int, amplitude: Float = 0.5) -> [Float] {
    (0..<count).map { k in
        amplitude * Float(sin(2.0 * Double.pi * frequency * Double(k) / sampleRate))
    }
}

/// A 16-frame x 12-bin window filled from a closure.
private func window(_ fill: (_ frame: Int, _ bin: Int) -> Float) -> [[Float]] {
    (0..<ChromaFingerprinter.windowFrameCount).map { frame in
        (0..<ChromaFingerprinter.chromaBinCount).map { bin in fill(frame, bin) }
    }
}

/// Frequency of FFT bin `bin` for the 4096-point transform at 11025 Hz.
private func binFrequency(_ bin: Int) -> Double {
    Double(bin) * sampleRate / Double(ChromaFingerprinter.frameSize)
}

@Suite("ChromaFingerprinter stages (playhead-xsdz.26)")
struct ChromaFingerprinterTests {

    // MARK: Windowing

    @Test("periodic Hann window: zero head, unit midpoint, periodic symmetry")
    func hannWindowShape() {
        let n = ChromaFingerprinter.frameSize
        let w = ChromaFingerprinter.hannWindow(length: n)
        #expect(w.count == n)
        #expect(abs(w[0]) < 1e-6)
        #expect(abs(w[n / 2] - 1.0) < 1e-6)
        // Periodic Hann: w[k] == w[N-k] for 1 <= k < N.
        for k in [1, 37, n / 4, n / 3, n / 2 - 1] {
            #expect(abs(w[k] - w[n - k]) < 1e-5, "asymmetry at k=\(k)")
        }
        // Strictly rising over the first half.
        #expect(w[1] > w[0] && w[n / 4] > w[n / 8])
    }

    // MARK: FFT wrapper

    @Test("power spectrum of a bin-centered sinusoid peaks at exactly that bin")
    func powerSpectrumSinusoidPeak() {
        let n = ChromaFingerprinter.frameSize
        for bin in [100, 517, 1500] {
            let frame = sinusoid(frequency: binFrequency(bin), count: n)
            let spectrum = ChromaFingerprinter.powerSpectrum(frame: frame)
            #expect(spectrum.count == n / 2 + 1)
            let peak = spectrum.indices.max { spectrum[$0] < spectrum[$1] }
            #expect(peak == bin, "expected peak at \(bin), got \(String(describing: peak))")
            // Energy is concentrated: the peak dwarfs a far-away bin.
            #expect(spectrum[bin] > 1e3 * max(spectrum[bin + 50], 1e-20))
        }
    }

    @Test("power spectrum of silence is (near) zero everywhere")
    func powerSpectrumSilence() {
        let spectrum = ChromaFingerprinter.powerSpectrum(
            frame: [Float](repeating: 0, count: ChromaFingerprinter.frameSize))
        #expect(spectrum.allSatisfy { $0.magnitude < 1e-12 })
    }

    // MARK: Chroma folding

    @Test("pitch-class math: A octaves fold to class 0; neighbors to 1 and 2; range enforced")
    func chromaClassMath() {
        // A at any octave -> class 0 (octave invariance of the fold).
        for f in [55.0, 110.0, 220.0, 440.0, 880.0, 1760.0, 3520.0] {
            #expect(ChromaFingerprinter.chromaClass(forFrequency: f) == 0, "A@\(f)")
        }
        #expect(ChromaFingerprinter.chromaClass(forFrequency: 466.16) == 1)   // A#4
        #expect(ChromaFingerprinter.chromaClass(forFrequency: 493.88) == 2)   // B4
        #expect(ChromaFingerprinter.chromaClass(forFrequency: 261.63) == 3)   // C4
        // Outside [55, 3520] -> nil (DC/rumble and near-Nyquist excluded).
        for f in [0.0, 10.0, 54.9, 3520.5, 5000.0, 5512.5] {
            #expect(ChromaFingerprinter.chromaClass(forFrequency: f) == nil, "out-of-range \(f)")
        }
    }

    @Test("pure tones land in the right chroma bin; octaves land in the SAME bin")
    func chromaVectorPureTones() {
        func dominantBin(frequency: Double) -> Int? {
            let frame = sinusoid(frequency: frequency, count: ChromaFingerprinter.frameSize)
            let chroma = ChromaFingerprinter.chromaVector(
                powerSpectrum: ChromaFingerprinter.powerSpectrum(frame: frame))
            #expect(chroma.count == ChromaFingerprinter.chromaBinCount)
            return chroma.indices.max { chroma[$0] < chroma[$1] }
        }
        #expect(dominantBin(frequency: 440.0) == 0)     // A4
        #expect(dominantBin(frequency: 880.0) == 0)     // A5 — octave invariance
        #expect(dominantBin(frequency: 220.0) == 0)     // A3 — octave invariance
        #expect(dominantBin(frequency: 493.88) == 2)    // B4
        #expect(dominantBin(frequency: 261.63) == 3)    // C4
    }

    // MARK: Chroma image (normalize + smooth)

    @Test("chromagram frame-count math over input lengths")
    func chromagramFrameCount() {
        let frame = ChromaFingerprinter.frameSize
        let hop = ChromaFingerprinter.hopSize
        #expect(ChromaFingerprinter.chromagram(samples: []).isEmpty)
        #expect(ChromaFingerprinter.chromagram(
            samples: [Float](repeating: 0, count: frame - 1)).isEmpty)
        #expect(ChromaFingerprinter.chromagram(
            samples: [Float](repeating: 0, count: frame)).count == 1)
        #expect(ChromaFingerprinter.chromagram(
            samples: [Float](repeating: 0, count: frame + hop - 1)).count == 1)
        #expect(ChromaFingerprinter.chromagram(
            samples: [Float](repeating: 0, count: frame + 20 * hop)).count == 21)
    }

    @Test("steady-tone chromagram frames are unit-normalized; silence stays all-zero")
    func chromagramNormalization() {
        let toneFrames = ChromaFingerprinter.chromagram(
            samples: sinusoid(
                frequency: 440.0,
                count: ChromaFingerprinter.frameSize + 20 * ChromaFingerprinter.hopSize))
        #expect(toneFrames.count == 21)
        for (i, frame) in toneFrames.enumerated() {
            let norm = sqrt(frame.reduce(Float(0)) { $0 + $1 * $1 })
            // A steady tone yields identical frames, so 3-frame smoothing
            // preserves the unit norm (including at the edges).
            #expect(abs(norm - 1.0) < 1e-3, "frame \(i) norm \(norm)")
        }
        let silent = ChromaFingerprinter.chromagram(
            samples: [Float](repeating: 0, count: ChromaFingerprinter.frameSize + 5 * ChromaFingerprinter.hopSize))
        #expect(silent.allSatisfy { $0.allSatisfy { $0 == 0 } })
    }

    @Test("3-frame centered smoothing with shrinking edges (step input)")
    func chromaSmoothingSemantics() {
        func frame(_ v2: Float, _ v7: Float) -> [Float] {
            var f = [Float](repeating: 0, count: ChromaFingerprinter.chromaBinCount)
            f[2] = v2
            f[7] = v7
            return f
        }
        // Step-like values so centered/causal/shifted averages all differ.
        let raw = [frame(0, 1), frame(1, 1), frame(4, 1), frame(10, 1)]
        let smoothed = ChromaFingerprinter.smoothedChromagram(raw)
        #expect(smoothed.count == 4)
        // Centered 3-frame average with shrinking edges:
        // s0=(0+1)/2, s1=(0+1+4)/3, s2=(1+4+10)/3, s3=(4+10)/2.
        let expectedBin2: [Float] = [0.5, 5.0 / 3.0, 5.0, 7.0]
        for (i, expected) in expectedBin2.enumerated() {
            #expect(abs(smoothed[i][2] - expected) < 1e-6, "bin 2, frame \(i)")
            // A steady bin is unchanged by smoothing (incl. at the edges).
            #expect(abs(smoothed[i][7] - 1.0) < 1e-6, "bin 7, frame \(i)")
        }
        // Bins that were zero everywhere stay zero (per-bin independence).
        #expect(smoothed.allSatisfy { $0[0] == 0 && $0[11] == 0 })
        // Edge cases: empty and single-frame inputs pass through unchanged.
        #expect(ChromaFingerprinter.smoothedChromagram([]).isEmpty)
        #expect(ChromaFingerprinter.smoothedChromagram([frame(3, 9)]) == [frame(3, 9)])
    }

    // MARK: Filter responses

    private func filter(
        _ kind: ChromaFilter.Kind,
        t: Int = 0, w: Int = 16, c: Int = 0, h: Int = 12
    ) -> ChromaFilter {
        ChromaFilter(kind: kind, timeStart: t, timeWidth: w, chromaStart: c, chromaHeight: h, t1: 0, t2: 0, t3: 0)
    }

    @Test("totalEnergy = plain area sum")
    func filterTotalEnergy() {
        let ones = window { _, _ in 1 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.totalEnergy), window: ones) - 192) < 1e-4)
        // Region restriction: only bins 4..<8 counted.
        let response = ChromaFingerprinter.filterResponse(
            filter(.totalEnergy, c: 4, h: 4), window: window { _, bin in bin < 4 ? 9 : 1 })
        #expect(abs(response - 64) < 1e-4)
    }

    @Test("chromaHalves = lower chroma half minus upper chroma half")
    func filterChromaHalves() {
        let lowerHot = window { _, bin in bin < 6 ? 1 : 0 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.chromaHalves), window: lowerHot) - 96) < 1e-4)
        let upperHot = window { _, bin in bin < 6 ? 0 : 1 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.chromaHalves), window: upperHot) + 96) < 1e-4)
        // Offset sub-band (bins 3..<9, split at 6), values outside ignored.
        let response = ChromaFingerprinter.filterResponse(
            filter(.chromaHalves, c: 3, h: 6),
            window: window { _, bin in
                if bin >= 3 && bin < 6 { return 2 }
                if bin >= 6 && bin < 9 { return 1 }
                return 7
            })
        #expect(abs(response - 48) < 1e-4)  // 16*3*2 - 16*3*1
    }

    @Test("timeHalves = first time half minus second time half")
    func filterTimeHalves() {
        let earlyHot = window { frame, _ in frame < 8 ? 1 : 0 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.timeHalves), window: earlyHot) - 96) < 1e-4)
        let lateHot = window { frame, _ in frame < 8 ? 0 : 1 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.timeHalves), window: lateHot) + 96) < 1e-4)
    }

    @Test("quadrants = (early-low + late-high) minus (early-high + late-low)")
    func filterQuadrants() {
        let checker = window { frame, bin in (frame < 8) == (bin < 6) ? Float(1) : 0 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.quadrants), window: checker) - 96) < 1e-4)
        let inverse = window { frame, bin in (frame < 8) == (bin < 6) ? Float(0) : 1 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.quadrants), window: inverse) + 96) < 1e-4)
    }

    @Test("chromaCenterFlanks = middle chroma half minus quarter flanks")
    func filterChromaCenterFlanks() {
        let centerHot = window { _, bin in bin >= 3 && bin < 9 ? Float(1) : 0 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.chromaCenterFlanks), window: centerHot) - 96) < 1e-4)
        let flankHot = window { _, bin in bin < 3 || bin >= 9 ? Float(1) : 0 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.chromaCenterFlanks), window: flankHot) + 96) < 1e-4)
    }

    @Test("timeCenterFlanks = middle time span minus quarter flanks")
    func filterTimeCenterFlanks() {
        let centerHot = window { frame, _ in frame >= 4 && frame < 12 ? Float(1) : 0 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.timeCenterFlanks), window: centerHot) - 96) < 1e-4)
        let flankHot = window { frame, _ in frame < 4 || frame >= 12 ? Float(1) : 0 }
        #expect(abs(ChromaFingerprinter.filterResponse(filter(.timeCenterFlanks), window: flankHot) + 96) < 1e-4)
    }

    // MARK: Quantization / Gray code

    @Test("2-bit quantization is Gray-coded 00,01,11,10 with strict thresholds")
    func quantizeGrayCode() {
        func q(_ r: Float) -> UInt32 {
            ChromaFingerprinter.quantize(response: r, t1: 1, t2: 2, t3: 3)
        }
        #expect(q(-100) == 0b00)
        #expect(q(0.5) == 0b00)
        #expect(q(1.5) == 0b01)
        #expect(q(2.5) == 0b11)
        #expect(q(3.5) == 0b10)
        #expect(q(100) == 0b10)
        // Boundary: a response EXACTLY at a threshold stays in the lower level.
        #expect(q(1.0) == 0b00)
        #expect(q(2.0) == 0b01)
        #expect(q(3.0) == 0b11)
        // Adjacent levels differ by exactly one bit (the Gray property).
        let codes: [UInt32] = [q(0.5), q(1.5), q(2.5), q(3.5)]
        for (a, b) in zip(codes, codes.dropFirst()) {
            #expect((a ^ b).nonzeroBitCount == 1)
        }
    }

    // MARK: Packing + stream assembly

    @Test("subfingerprint packs filter i's Gray bits at bits (2i+1, 2i)")
    func subfingerprintPacking() {
        // A deterministic, irregular window so filters spread across levels.
        let testWindow = window { frame, bin in
            Float(sin(Double(frame) * 0.71 + Double(bin) * 1.37)) * 0.5 + 0.5
        }
        var expected: UInt32 = 0
        for (i, f) in ChromaFingerprinter.filters.enumerated() {
            let code = ChromaFingerprinter.quantize(
                response: ChromaFingerprinter.filterResponse(f, window: testWindow),
                t1: f.t1, t2: f.t2, t3: f.t3)
            expected |= code << (2 * i)
        }
        #expect(ChromaFingerprinter.subfingerprint(window: testWindow) == expected)
    }

    @Test("subfingerprint stream equals per-window packing at every position")
    func streamMatchesPerWindowComputation() {
        // 40 synthetic chroma frames -> 25 subfingerprints.
        let frames: [[Float]] = (0..<40).map { frame in
            (0..<12).map { bin in
                Float(sin(Double(frame) * 0.53 + Double(bin) * 0.91)) * 0.5 + 0.5
            }
        }
        let stream = ChromaFingerprinter.subfingerprints(chromagram: frames)
        #expect(stream.count == 40 - ChromaFingerprinter.windowFrameCount + 1)
        for position in stream.indices {
            let windowSlice = Array(frames[position..<(position + ChromaFingerprinter.windowFrameCount)])
            #expect(stream[position] == ChromaFingerprinter.subfingerprint(window: windowSlice),
                    "mismatch at position \(position)")
        }
    }

    @Test("fingerprint count math: max(0, frames - 15)")
    func fingerprintCountMath() {
        let frame = ChromaFingerprinter.frameSize
        let hop = ChromaFingerprinter.hopSize
        func count(samples: Int) -> Int {
            ChromaFingerprinter.fingerprint(
                monoSamples11025: sinusoid(frequency: 330, count: samples)).count
        }
        #expect(count(samples: 0) == 0)
        #expect(count(samples: frame) == 0)                 // 1 frame < 16
        #expect(count(samples: frame + 14 * hop) == 0)      // 15 frames
        #expect(count(samples: frame + 15 * hop) == 1)      // 16 frames
        #expect(count(samples: frame + 40 * hop) == 26)     // 41 frames
    }

    @Test("determinism: identical input -> identical stream, sequential and concurrent")
    func fingerprintDeterminism() async {
        // Multi-tone signal so all pipeline paths carry non-trivial data.
        let count = ChromaFingerprinter.frameSize + 60 * ChromaFingerprinter.hopSize
        let samples: [Float] = (0..<count).map { k in
            let t = Double(k) / sampleRate
            return Float(
                0.4 * sin(2 * .pi * 220.0 * t)
                + 0.3 * sin(2 * .pi * 523.25 * t)
                + 0.2 * sin(2 * .pi * (1000.0 + 400.0 * sin(t * 0.7)) * t))
        }
        let first = ChromaFingerprinter.fingerprint(monoSamples11025: samples)
        let second = ChromaFingerprinter.fingerprint(monoSamples11025: samples)
        #expect(first == second)
        #expect(!first.isEmpty)
        async let a = Task.detached { ChromaFingerprinter.fingerprint(monoSamples11025: samples) }.value
        async let b = Task.detached { ChromaFingerprinter.fingerprint(monoSamples11025: samples) }.value
        let (concurrentA, concurrentB) = await (a, b)
        #expect(concurrentA == first)
        #expect(concurrentB == first)
    }

    // MARK: Corrupt-input containment

    @Test("non-finite samples are contained locally and never poison downstream windows")
    func nonFiniteSampleContainment() {
        let frame = ChromaFingerprinter.frameSize
        let hop = ChromaFingerprinter.hopSize
        let count = frame + 120 * hop
        let clean = sinusoid(frequency: 261.63, count: count)
        var corrupt = clean
        // Three corruption flavors. The FINITE overflow sample is the
        // load-bearing case: the FFT stays NaN-free but squaring to power
        // overflows Float to +Inf, so the chroma norm comes out Inf and
        // only the isFinite guard stops Inf/Inf = NaN from entering the
        // chroma image (verified to poison EVERY downstream window without
        // the guard). Raw Inf and NaN collapse to NaN inside the FFT/norm
        // math and take the same zeroing branch. The overflow sample MUST
        // sit in STFT frames of its own — sharing frames with Inf/NaN
        // would collapse its norm to NaN and mask the overflow path.
        let badIndices = [frame + 30 * hop, frame + 60 * hop, frame + 60 * hop + 5]
        corrupt[badIndices[0]] = 3e19
        corrupt[badIndices[1]] = .infinity
        corrupt[badIndices[2]] = .nan

        // No NaN/Inf may survive into the chroma image: a single non-finite
        // value in ONE frame would otherwise propagate through the integral
        // image's prefix sums into EVERY later window position, silently
        // zero-quantizing the rest of the episode.
        let chromagram = ChromaFingerprinter.chromagram(samples: corrupt)
        #expect(chromagram.allSatisfy { $0.allSatisfy(\.isFinite) })

        let cleanStream = ChromaFingerprinter.fingerprint(monoSamples11025: clean)
        let corruptStream = ChromaFingerprinter.fingerprint(monoSamples11025: corrupt)
        #expect(corruptStream.count == cleanStream.count)
        // Corruption handling is deterministic.
        #expect(corruptStream == ChromaFingerprinter.fingerprint(monoSamples11025: corrupt))

        // Windows whose span touches a corrupted STFT frame (±1 smoothing
        // bleed) may legitimately differ; every OTHER subfingerprint must
        // be bit-identical to the clean run.
        let frameCount = (count - frame) / hop + 1
        var contaminatedFrames = Set<Int>()
        for index in badIndices {
            for f in 0..<frameCount where f * hop <= index && index < f * hop + frame {
                contaminatedFrames.formUnion((f - 1)...(f + 1))  // smoothing bleed
            }
        }
        let contaminatedPositions = Set(contaminatedFrames.flatMap { f in
            (f - (ChromaFingerprinter.windowFrameCount - 1))...f
        })
        let mismatches = cleanStream.indices.filter { position in
            !contaminatedPositions.contains(position) && corruptStream[position] != cleanStream[position]
        }
        #expect(mismatches.isEmpty,
                "corruption leaked beyond its windows: clean positions changed at \(mismatches)")
    }

    // MARK: Granularity pin (differ interop)

    @Test("secondsPerFingerprint is the exact rational hop/sampleRate in the 0.12-0.125 class")
    func secondsPerFingerprintPinned() {
        // The EXACT value the rediff validation suite passes to
        // RediffPrototype.rediff as secondsPerFpA/B. If this ever drifts,
        // the differ's knob tuning (minGapFps math etc.) must be revisited.
        #expect(ChromaFingerprinter.secondsPerFingerprint == 1365.0 / 11025.0)
        #expect(ChromaFingerprinter.secondsPerFingerprint
                == Double(ChromaFingerprinter.hopSize) / Double(ChromaFingerprinter.requiredSampleRate))
        #expect(ChromaFingerprinter.secondsPerFingerprint >= 0.12)
        #expect(ChromaFingerprinter.secondsPerFingerprint <= 0.125)
    }

    // MARK: Filter-bank invariants

    @Test("filter bank: exactly 16 filters, geometry in bounds, thresholds ascending, all arrangements present")
    func filterBankInvariants() {
        let bank = ChromaFingerprinter.filters
        #expect(bank.count == 16)  // 16 filters x 2 bits = 32-bit subfingerprint
        for (i, f) in bank.enumerated() {
            #expect(f.timeStart >= 0 && f.timeWidth >= 1
                    && f.timeStart + f.timeWidth <= ChromaFingerprinter.windowFrameCount,
                    "filter \(i) time geometry")
            #expect(f.chromaStart >= 0 && f.chromaHeight >= 1
                    && f.chromaStart + f.chromaHeight <= ChromaFingerprinter.chromaBinCount,
                    "filter \(i) chroma geometry")
            #expect(f.t1 <= f.t2 && f.t2 <= f.t3, "filter \(i) thresholds not ascending")
            switch f.kind {
            case .chromaHalves, .quadrants:
                #expect(f.chromaHeight.isMultiple(of: 2), "filter \(i) needs even chroma height")
            case .chromaCenterFlanks:
                #expect(f.chromaHeight.isMultiple(of: 4), "filter \(i) needs chroma height % 4 == 0")
            case .timeCenterFlanks:
                #expect(f.timeWidth.isMultiple(of: 4), "filter \(i) needs time width % 4 == 0")
            case .timeHalves:
                #expect(f.timeWidth.isMultiple(of: 2), "filter \(i) needs even time width")
            case .totalEnergy:
                break
            }
            if f.kind == .quadrants {
                #expect(f.timeWidth.isMultiple(of: 2), "filter \(i) needs even time width")
            }
            // Calibrated thresholds must be non-degenerate: a filter whose
            // three thresholds collapse to one value wastes its 2 bits.
            #expect(f.t1 < f.t3, "filter \(i) thresholds degenerate")
        }
        // All six arrangements (the five published + the [OWN CHOICE] sixth)
        // are represented in the bank.
        let kinds = Set(bank.map(\.kind))
        #expect(kinds == Set(ChromaFilter.Kind.allCases))
    }
}

// MARK: - Algorithm identity pin (persistence contract, feeds playhead-xsdz.27)

/// Pins EVERY constant that determines the emitted fingerprint bits. If any
/// expectation below fails, the fingerprint ALGORITHM changed — persisted
/// fingerprints from earlier builds are stale. The SAME change MUST bump
/// `ChromaFingerprinter.algorithmVersion` (so stores can detect staleness),
/// update these pins, and re-run the corpus validation matrix
/// (docs/xsdz26-fingerprinter-validation.md).
///
/// Quantization semantics (strict thresholds, Gray code 00/01/11/10) and
/// bit-packing order are pinned by `quantizeGrayCode` and
/// `subfingerprintPacking` above; the constants live here. The STFT analysis
/// window — bit-determining but only loosely shape-checked by
/// `hannWindowShape` (Hamming/Blackman/Hann² all satisfy those shape checks
/// while changing bits) — is pinned to the exact periodic-Hann values by
/// `windowPinned` below.
@Suite("ChromaFingerprinter algorithm identity pin (playhead-xsdz.26)")
struct ChromaAlgorithmIdentityPinTests {

    @Test("algorithmVersion 1 <=> exactly this pinned configuration")
    func versionPinned() {
        #expect(ChromaFingerprinter.algorithmVersion == 1)
    }

    @Test("pipeline constants pinned")
    func pipelineConstantsPinned() {
        #expect(ChromaFingerprinter.requiredSampleRate == 11025)
        #expect(ChromaFingerprinter.frameSize == 4096)
        #expect(ChromaFingerprinter.hopSize == 1365)
        // chromaBinCount is bit-determining THREE ways: chroma-vector length,
        // AND (in 12-TET) the pitch fold's octave divisor and wrap-around
        // modulus in `chromaClass` (both reference chromaBinCount, not bare
        // `12`s). A change here reassigns FFT bins to chroma classes and
        // rewrites emitted bits, so this single pin also locks the fold.
        #expect(ChromaFingerprinter.chromaBinCount == 12)
        #expect(ChromaFingerprinter.windowFrameCount == 16)
        #expect(ChromaFingerprinter.minChromaFrequency == 55.0)
        #expect(ChromaFingerprinter.maxChromaFrequency == 3520.0)
        // Bit-determining: the pitch-class fold's tuning anchor. A shift here
        // (e.g. A=442) reassigns FFT bins near class boundaries to adjacent
        // chroma classes — chromaClassMath's spot tones do NOT constrain it.
        #expect(ChromaFingerprinter.referenceTuningFrequency == 440.0)
        // Bit-determining too: decides which near-silent frames zero out
        // instead of normalizing.
        #expect(ChromaFingerprinter.silenceNormEpsilon == 1e-10)
    }

    @Test("calibrated filter bank pinned bit-for-bit")
    func filterBankPinned() {
        // Deliberate golden copy of the calibrated bank (identical Float
        // literals produce identical bits). Any drift here is an algorithm
        // change — see the suite comment.
        let expected: [ChromaFilter] = [
            ChromaFilter(kind: .totalEnergy, timeStart: 0, timeWidth: 16, chromaStart: 0, chromaHeight: 12, t1: 35.614025, t2: 38.617001, t3: 41.071117),
            ChromaFilter(kind: .totalEnergy, timeStart: 0, timeWidth: 16, chromaStart: 0, chromaHeight: 4, t1: 10.668667, t2: 13.205434, t3: 15.558352),
            ChromaFilter(kind: .totalEnergy, timeStart: 0, timeWidth: 16, chromaStart: 4, chromaHeight: 4, t1: 9.392507, t2: 11.859087, t3: 14.215107),
            ChromaFilter(kind: .totalEnergy, timeStart: 0, timeWidth: 16, chromaStart: 8, chromaHeight: 4, t1: 10.928547, t2: 13.397732, t3: 15.897790),
            ChromaFilter(kind: .chromaHalves, timeStart: 0, timeWidth: 16, chromaStart: 0, chromaHeight: 12, t1: -5.609742, t2: -0.708112, t3: 4.133924),
            ChromaFilter(kind: .chromaHalves, timeStart: 0, timeWidth: 16, chromaStart: 0, chromaHeight: 6, t1: -1.775435, t2: 1.041729, t3: 4.178531),
            ChromaFilter(kind: .chromaHalves, timeStart: 0, timeWidth: 16, chromaStart: 6, chromaHeight: 6, t1: -4.208940, t2: -1.441194, t3: 1.496095),
            ChromaFilter(kind: .chromaHalves, timeStart: 0, timeWidth: 16, chromaStart: 3, chromaHeight: 6, t1: -2.820365, t2: -0.238321, t3: 2.286964),
            ChromaFilter(kind: .timeHalves, timeStart: 0, timeWidth: 16, chromaStart: 0, chromaHeight: 12, t1: -1.780836, t2: 0.006326, t3: 1.833915),
            ChromaFilter(kind: .timeHalves, timeStart: 0, timeWidth: 16, chromaStart: 0, chromaHeight: 6, t1: -1.888546, t2: -0.006302, t3: 1.858517),
            ChromaFilter(kind: .timeHalves, timeStart: 0, timeWidth: 16, chromaStart: 6, chromaHeight: 6, t1: -2.083807, t2: 0.027044, t3: 2.029585),
            ChromaFilter(kind: .quadrants, timeStart: 0, timeWidth: 16, chromaStart: 0, chromaHeight: 12, t1: -3.520256, t2: -0.062775, t3: 3.505196),
            ChromaFilter(kind: .quadrants, timeStart: 0, timeWidth: 16, chromaStart: 2, chromaHeight: 8, t1: -2.692694, t2: -0.005825, t3: 2.669349),
            ChromaFilter(kind: .chromaCenterFlanks, timeStart: 0, timeWidth: 16, chromaStart: 0, chromaHeight: 12, t1: -7.619020, t2: -2.649128, t3: 2.292797),
            ChromaFilter(kind: .chromaCenterFlanks, timeStart: 0, timeWidth: 16, chromaStart: 2, chromaHeight: 8, t1: -3.740144, t2: -0.797891, t3: 2.148873),
            ChromaFilter(kind: .timeCenterFlanks, timeStart: 0, timeWidth: 16, chromaStart: 0, chromaHeight: 12, t1: -1.527566, t2: 0.000154, t3: 1.546575),
        ]
        #expect(ChromaFingerprinter.filters == expected)
    }

    @Test("STFT analysis window pinned to the exact periodic-Hann shape")
    func windowPinned() {
        // The window is bit-determining (it reweights spectral leakage before
        // the chroma fold). `hannWindowShape` only asserts loose shape
        // invariants that other windows also satisfy — verified: Hann², and
        // periodic Blackman both pass every hannWindowShape check while giving
        // w[N/4] = 0.25 / 0.34 instead of Hann's 0.50, which changes emitted
        // bits. This pin restates the periodic-Hann formula independently, so
        // a swap to any other window fails here and forces an algorithmVersion
        // bump. Any drift => algorithm change (see the suite comment).
        let n = ChromaFingerprinter.frameSize
        let w = ChromaFingerprinter.hannWindow(length: n)
        #expect(w.count == n)
        // Independent restatement of w[k] = 0.5·(1 − cos(2πk/N)).
        for k in [0, 1, 37, n / 8, n / 4, 3 * n / 8, n / 3, n / 2, n - 1] {
            let expected = Float(0.5 * (1.0 - cos(2.0 * Double.pi * Double(k) / Double(n))))
            #expect(abs(w[k] - expected) < 1e-6, "window[\(k)] \(w[k]) vs \(expected)")
        }
        // Exact landmarks unique to periodic Hann among the plausible windows
        // (Hamming w[0]=0.08; Hann² w[N/4]=0.25; Blackman w[N/4]=0.34):
        #expect(w[0] == 0.0)
        #expect(abs(w[n / 4] - 0.5) < 1e-6)
        #expect(abs(w[n / 2] - 1.0) < 1e-6)
    }

    /// GOLDEN OUTPUT PIN — the COMPLETENESS guarantee for the algorithm
    /// identity contract, and the closer for the recurring silent-staleness
    /// defect class (seven review rounds, R1–R7, each found ONE more
    /// bit-determining element a per-element pin had missed: scalar constants,
    /// the pitch-fold divisor/modulus, the A=440 tuning anchor, the silence
    /// epsilon, the Hann window). Per-element enumeration cannot PROVE it is
    /// complete; this pin closes the class BY CONSTRUCTION.
    ///
    /// It runs the PRODUCTION `fingerprint(...)` over a FIXED, fully-documented
    /// synthetic PCM input (`goldenSyntheticInput`) that exercises every
    /// bit-determining path — STFT window, chroma fold, per-frame
    /// normalization, the silence/zeroing branch, 3-frame smoothing across
    /// region edges, all six filter arrangements, quantization/Gray coding,
    /// and 32-bit packing — across 117 windows, then asserts the emitted
    /// `[UInt32]` matches a HARDCODED golden digest (exact count, an FNV-1a-64
    /// over the whole stream, an XOR-fold, the total popcount, and spot values
    /// at fixed indices). The FNV hash is order- and value-sensitive, so a
    /// different algorithm — including one that flips bits via an element NO
    /// per-element pin enumerates (e.g. swapping the magnitude-SQUARED power
    /// spectrum for magnitude, which the stage tests do not constrain) —
    /// cannot satisfy it.
    ///
    /// GOLDEN PROVENANCE: the constants below were generated ONCE by running
    /// the CURRENT code on the fixed input and pasting the observed values.
    /// This is the one legitimate "golden from the code under test": its
    /// PURPOSE is to FREEZE the current emitted bits, not to independently
    /// re-derive them. If this test fails, the algorithm CHANGED — the
    /// persisted fingerprints from earlier builds are stale. Do NOT edit the
    /// golden to match: bump `ChromaFingerprinter.algorithmVersion`,
    /// regenerate the golden, re-fingerprint persisted data, and NEVER
    /// Hamming-compare streams across versions. (The per-element pins above
    /// then localize WHICH element moved.) The one benign exception is the
    /// documented SAME-DEVICE limitation — a response within ~1 Float ulp of a
    /// threshold can quantize differently under a new libm/arch even with the
    /// algorithm unchanged; if a handful of spot indices drift with the
    /// per-element pins all green, confirm it is a toolchain move (not an
    /// algorithm edit) before regenerating. See
    /// docs/xsdz26-fingerprinter-validation.md, "Known limitations".
    @Test("golden output pin: production fingerprint(...) is bit-frozen (identity-class completeness)")
    func goldenOutputPinned() {
        let stream = ChromaFingerprinter.fingerprint(monoSamples11025: goldenSyntheticInput())
        let digest = GoldenDigest(stream: stream)
        // Frozen 2026-07-07 from the CURRENT code on `goldenSyntheticInput()`
        // (see GOLDEN PROVENANCE above). The repeated spot values are honest:
        // the steady chord/silence regions emit identical subfingerprints at
        // those positions. The FNV-1a-64 over the whole stream is the primary
        // completeness gate; xorFold/popcount/spots are auditable cross-checks.
        let golden = GoldenDigest(
            count: 117,
            fnv1a64: 0x12d8_98bf_c6bd_101b,
            xorFold: 0xf594_a710,
            totalPopcount: 1837,
            spots: [0x6fdd2a1c, 0x6fdd2a1c, 0x6fdd2a1c, 0x7fddf700, 0xe3dd2a08, 0x61752a08, 0x61752a08])
        #expect(digest.count == golden.count)
        #expect(digest.fnv1a64 == golden.fnv1a64,
                "stream hash drifted — the fingerprint algorithm changed (see doc)")
        #expect(digest.xorFold == golden.xorFold)
        #expect(digest.totalPopcount == golden.totalPopcount)
        #expect(digest.spots == golden.spots)
        // Single-shot completeness gate.
        #expect(digest == golden)
    }
}

// MARK: - Golden-pin fixtures (deterministic; documented for reproducibility)

/// Fixed synthetic PCM for `goldenOutputPinned`, reproducible from this
/// comment alone. 11025 Hz mono, t = k / 11025.
///
/// Length: 132 chroma frames => count = frameSize + 131·hopSize = 182,911
/// samples (~16.6 s) => 132 − 15 = 117 subfingerprints, spanning many windows.
///
/// Three regions (boundaries b1 = count·2/5 = 73164, b2 = count·3/5 = 109746)
/// deliberately exercise every bit-determining path:
///   [0, b1)       A-ish triad:  0.40·sin(2π·220 t) + 0.30·sin(2π·329.63 t)
///                               + 0.20·sin(2π·554.37 t)
///   [b1, b2)      silence (all zeros) — the ~3.3 s block (≫ one 4096-sample
///                 frame) forces whole STFT frames through the norm ≤ epsilon
///                 zeroing branch, and its edges exercise 3-frame smoothing.
///   [b2, count)   detuned triad with slow FM:
///                 0.50·sin(2π·440 t) + 0.25·sin(2π·659.25 t)
///                 + 0.15·sin(2π·(880 + 200·sin(2π·0.5 t)) t)
private func goldenSyntheticInput() -> [Float] {
    let sr = Double(ChromaFingerprinter.requiredSampleRate)
    let count = ChromaFingerprinter.frameSize + 131 * ChromaFingerprinter.hopSize
    let b1 = count * 2 / 5
    let b2 = count * 3 / 5
    return (0..<count).map { k in
        let t = Double(k) / sr
        if k < b1 {
            return Float(0.40 * sin(2 * .pi * 220.0 * t)
                + 0.30 * sin(2 * .pi * 329.63 * t)
                + 0.20 * sin(2 * .pi * 554.37 * t))
        } else if k < b2 {
            return 0
        } else {
            let fm = 880.0 + 200.0 * sin(2 * .pi * 0.5 * t)
            return Float(0.50 * sin(2 * .pi * 440.0 * t)
                + 0.25 * sin(2 * .pi * 659.25 * t)
                + 0.15 * sin(2 * .pi * fm * t))
        }
    }
}

/// A compact, bit-exact digest of a subfingerprint stream. The FNV-1a-64 hash
/// (over the little-endian bytes, index-ascending) is order- and
/// value-sensitive, so it cannot be satisfied by a reordered or altered
/// stream; the XOR-fold, popcount, and fixed spot values are human-auditable
/// cross-checks that also pin concrete positions.
private struct GoldenDigest: Equatable {
    let count: Int
    let fnv1a64: UInt64
    let xorFold: UInt32
    let totalPopcount: Int
    let spots: [UInt32]

    /// Fixed spot indices for a stream of `count` subfingerprints.
    static func spotIndices(count: Int) -> [Int] {
        [0, 1, count / 4, count / 2, 3 * count / 4, count - 2, count - 1]
    }

    init(stream: [UInt32]) {
        count = stream.count
        var hash: UInt64 = 0xcbf2_9ce4_8422_2325
        var xor: UInt32 = 0
        var pop = 0
        for value in stream {
            xor ^= value
            pop += value.nonzeroBitCount
            var byte = value
            for _ in 0..<4 {
                hash ^= UInt64(byte & 0xff)
                hash = hash &* 0x0000_0100_0000_01b3
                byte >>= 8
            }
        }
        fnv1a64 = hash
        xorFold = xor
        totalPopcount = pop
        spots = stream.isEmpty ? [] : GoldenDigest.spotIndices(count: stream.count).map { stream[$0] }
    }

    init(count: Int, fnv1a64: UInt64, xorFold: UInt32, totalPopcount: Int, spots: [UInt32]) {
        self.count = count
        self.fnv1a64 = fnv1a64
        self.xorFold = xorFold
        self.totalPopcount = totalPopcount
        self.spots = spots
    }
}
