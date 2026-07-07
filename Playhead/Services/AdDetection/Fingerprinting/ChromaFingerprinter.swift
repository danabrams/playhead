// ChromaFingerprinter.swift
// playhead-xsdz.26: clean-room chromaprint-CLASS audio fingerprinter.
//
// Produces the [UInt32] subfingerprint stream consumed by the rediff
// width-oracle differ (PlayheadTests/.../RediffSpike/RediffPrototype.swift,
// validated in the xsdz.16 spike) and, later, library self-fingerprinting.
//
// CLEAN-ROOM PROVENANCE (licensing-critical, pinned)
// --------------------------------------------------
// This implementation was written EXCLUSIVELY from published algorithm
// descriptions; the chromaprint C/C++ source (LGPL) was NOT read, fetched,
// or consulted at any point. Bit-compatibility with chromaprint is
// explicitly NOT a goal — alignment quality through the rediff differ is.
// Every detail the public sources leave unspecified is an independent
// engineering choice, marked [OWN CHOICE] below. See
// docs/xsdz26-cleanroom-notes.md for the full provenance notes.
//
// Sources (published descriptions only):
//  1. Lukáš Lalinský, "How does Chromaprint work?", oxygene.sk blog,
//     2011-01-18. https://oxygene.sk/2011/01/how-does-chromaprint-work/
//  2. Yan Ke, Derek Hoiem, Rahul Sukthankar, "Computer Vision for Music
//     Identification", CVPR 2005.
//     https://www.cs.cmu.edu/~rahuls/pub/cvpr2005-rahuls.pdf
//  3. AcoustID / Chromaprint project page (non-source docs):
//     https://acoustid.org/chromaprint
//  4. J. Haitsma, T. Kalker, "A Highly Robust Audio Fingerprinting System",
//     ISMIR 2002 (energy-difference subfingerprint bits; Hamming matching).
//  5. M. Bartsch, G. Wakefield, "Audio Thumbnailing of Popular Music Using
//     Chroma-Based Representations" (12-pitch-class chroma folding).
//
// Pipeline (source 1 unless marked):
//   mono 11025 Hz PCM
//     -> STFT, frame 4096 (0.371 s), 2/3 overlap => hop 1365 [OWN CHOICE:
//        floor(4096/3); source 1 gives "2/3 overlap" without the exact hop]
//     -> fold power spectrum into 12 pitch-class (chroma) bins (source 5)
//     -> per-frame normalization + temporal smoothing [OWN CHOICE below]
//     -> sliding 16-frame x 12-bin window; 16 Haar-like area-difference
//        filters (source 2's rectangle-filter family)
//     -> each response quantized to 2 bits via 3 fixed per-filter
//        thresholds, Gray-coded (source 1)
//     -> packed 32-bit subfingerprint per window position.
//
// DESIGN FOR THE DIFFER: run matching in RediffPrototype is anchor-EXACT
// (a run needs at least one exact 32-bit match to seed it), so the filter
// bank deliberately favors large-area, full-window-width, heavily smoothed
// filters (source 2's own guidance: large time extents average out noise
// and codec distortion) to make exact matches frequent between two encodes
// of the same content.
//
// PURITY: value types + static pure functions only. Deterministic — no
// randomness, no time, no global mutable state. Accelerate (vDSP FFT) is
// the only dependency beyond Foundation. Nothing in production references
// this yet (integration is a later bead).

import Accelerate
import Foundation

// MARK: - ChromaFilter

/// One Haar-like rectangle filter over the sliding 16-frame x 12-bin chroma
/// window, plus its three fixed quantization thresholds.
///
/// Geometry is expressed window-relative: `timeStart/timeWidth` in frames
/// (0..<16 along the sliding axis), `chromaStart/chromaHeight` in chroma
/// bins (0..<12). The `kind` selects one of the published area arrangements
/// (source 2 Figure 2) plus one [OWN CHOICE] arrangement; see `Kind`.
struct ChromaFilter: Sendable, Equatable {

    /// Area arrangement classes. Source 1 says chromaprint's family has six
    /// arrangements; source 2 Figure 2 publishes five. The sixth here —
    /// `totalEnergy` — is an [OWN CHOICE] (plain summed area, the degenerate
    /// "compare region against fixed thresholds" arrangement).
    enum Kind: Sendable, Equatable, CaseIterable {
        /// [OWN CHOICE] Sum over the whole region.
        case totalEnergy
        /// Source 2 (a): lower-chroma half minus upper-chroma half.
        case chromaHalves
        /// Source 2 (b): first-time half minus second-time half.
        case timeHalves
        /// Source 2 (c): checkerboard quadrants (TL+BR) - (TR+BL), where
        /// "TL" is early-time/low-chroma.
        case quadrants
        /// Source 2 (d): middle chroma band minus outer flanks (band peak).
        case chromaCenterFlanks
        /// Source 2 (e): middle time span minus outer flanks (time peak).
        case timeCenterFlanks
    }

    let kind: Kind
    /// First frame of the region within the 16-frame window (0-based).
    let timeStart: Int
    /// Region width in frames. Halves/quadrants kinds need it even;
    /// `timeCenterFlanks` needs it divisible by 4 (w/4 flanks, w/2 center).
    let timeWidth: Int
    /// First chroma bin of the region (0-based, 0..<12).
    let chromaStart: Int
    /// Region height in chroma bins. Halves/quadrants kinds need it even;
    /// `chromaCenterFlanks` needs it divisible by 4.
    let chromaHeight: Int
    /// Quantization thresholds, ascending: response r maps to Gray-coded
    /// level = |{ t in (t1,t2,t3) : t < r }|  (strict; a response exactly
    /// equal to a threshold stays in the LOWER level). [OWN CHOICE]
    let t1: Float
    let t2: Float
    let t3: Float
}

// MARK: - ChromaFingerprinter

/// Clean-room chromaprint-class fingerprinter. Pure static functions over
/// mono 11025 Hz Float PCM; see file header for provenance and pipeline.
enum ChromaFingerprinter {

    // MARK: Fixed pipeline constants (source 1 unless marked)

    /// Input must already be mono at this rate. [OWN CHOICE]: resampling is
    /// the caller's job (AVAudioConverter upstream); keeping AVFoundation
    /// out of this file keeps it pure and dependency-free.
    static let requiredSampleRate: Int = 11025

    /// STFT frame length (0.371 s at 11025 Hz). 2^12 — see `log2FrameSize`.
    static let frameSize: Int = 4096

    /// Radix-2 FFT exponent. DERIVED from `frameSize` (not an independent
    /// literal) so the transform length can never silently desync from the
    /// frame length: `1 << log2FrameSize == frameSize` by construction.
    /// `frameSize` is pinned at 4096 (a power of two), so this is 12.
    /// Bit-determining, but non-independently-settable (bucket C).
    private static let log2FrameSize: vDSP_Length = vDSP_Length(frameSize.trailingZeroBitCount)

    /// STFT hop. Source 1: "2/3 overlap"; the exact hop is unspecified.
    /// [OWN CHOICE]: floor(4096/3) = 1365 samples.
    static let hopSize: Int = 1365

    /// Chroma (pitch-class) bin count — "notes, not octaves". In 12-TET this
    /// is ALSO the pitch-fold's octave divisor and wrap-around modulus (see
    /// `chromaClass`), so it is bit-determining on both counts; pinned by
    /// `pipelineConstantsPinned`.
    static let chromaBinCount: Int = 12

    /// Sliding classification window length in chroma frames.
    static let windowFrameCount: Int = 16

    /// Exact seconds per subfingerprint = hop / sampleRate = 1365/11025
    /// ≈ 0.123809… s — the ~0.12–0.125 s/fp class the differ was
    /// validated at. Exposed so callers pass the EXACT rational-derived
    /// value to the differ (granularity-pair pinned by test).
    static let secondsPerFingerprint: Double =
        Double(hopSize) / Double(requiredSampleRate)

    /// Chroma fold frequency range. [OWN CHOICE]: 55 Hz (A1) through
    /// 3520 Hz (A7) inclusive — excludes DC/rumble below speech
    /// fundamentals and the noisy near-Nyquist top where MP3/AAC codecs
    /// diverge most. Source 1 does not publish chromaprint's range.
    static let minChromaFrequency: Double = 55.0
    static let maxChromaFrequency: Double = 3520.0

    /// Reference tuning frequency (concert A4) anchoring the pitch-class
    /// fold: class = round(chromaBinCount·log2(f / referenceTuningFrequency))
    /// mod chromaBinCount.
    /// Standard A=440. Internal (not a bare literal in `chromaClass`) and
    /// pinned because it is bit-determining: a different anchor (e.g. A=442)
    /// reassigns FFT bins near class boundaries to adjacent chroma classes
    /// (~106 in-range bins shift for a 440→442 move), altering emitted bits
    /// while `chromaClassMath`'s spot frequencies stay green — exactly the
    /// silent-staleness hole the identity pin exists to close.
    static let referenceTuningFrequency: Double = 440.0

    /// [OWN CHOICE] A chroma frame whose Euclidean norm is at or below this
    /// stays all-zero (silence) instead of being normalized — normalizing
    /// numerical dust would amplify codec noise into arbitrary bits.
    /// Internal (not private) because it determines emitted bits on
    /// near-silent frames, so the algorithm identity pin asserts it.
    static let silenceNormEpsilon: Float = 1e-10

    // MARK: Algorithm identity (persistence contract)

    /// ALGORITHM VERSION — the staleness contract for PERSISTED
    /// fingerprints (the playhead-xsdz.27 store and any other persister).
    ///
    /// Fingerprint streams are only comparable when produced by the same
    /// algorithm: bump this constant on ANY change that can alter emitted
    /// bits — the pipeline constants above, the chroma fold range, the STFT
    /// analysis window (`hannWindow`, pinned by `windowPinned`), the
    /// power-spectrum energy (magnitude-squared) choice,
    /// normalization/smoothing semantics, the filter-bank geometry or
    /// thresholds, quantization/Gray coding, or bit packing. A store MUST
    /// persist this value alongside the fingerprints and treat any mismatch
    /// as stale (re-fingerprint; NEVER Hamming-compare streams across
    /// versions — cross-version comparison degrades silently into
    /// misalignment rather than failing loudly).
    /// `ChromaAlgorithmIdentityPinTests` pins every version-relevant
    /// constant so a behavioral change cannot land without touching the pin
    /// (whose comment requires bumping this version in the same change).
    /// The per-element pins localize WHICH element changed;
    /// `goldenOutputPinned` is the COMPLETENESS backstop — it freezes the
    /// actual emitted `[UInt32]` for a fixed synthetic input, so ANY
    /// bit-determining change (even one no per-element pin happens to
    /// enumerate — e.g. the magnitude-squared power choice, which the stage
    /// tests do not constrain) fails it and forces this version bump.
    ///
    /// SAME-DEVICE CONTRACT (also load-bearing for persistence): quantized
    /// bits are reproducible on the device+OS that produced them, but libm
    /// (`log2`/`cos`) and Float rounding may differ across architectures or
    /// OS versions, so a response within ~1 Float ulp of a threshold can
    /// quantize differently elsewhere. Persisted fingerprints are
    /// same-device artifacts; do NOT sync or share them across devices or
    /// users without revalidating (docs/xsdz26-fingerprinter-validation.md,
    /// "Known limitations").
    static let algorithmVersion: UInt32 = 1

    // MARK: Top-level entry

    /// Fingerprint mono 11025 Hz PCM into the 32-bit subfingerprint stream.
    ///
    /// Output count = max(0, chromaFrames - 15) where
    /// chromaFrames = (samples.count - frameSize)/hopSize + 1 (0 when the
    /// input is shorter than one frame). Subfingerprint i covers frames
    /// i..<i+16 and is anchored at time i * secondsPerFingerprint.
    static func fingerprint(monoSamples11025 samples: [Float]) -> [UInt32] {
        subfingerprints(chromagram: chromagram(samples: samples))
    }

    // MARK: Stage 1 — STFT

    /// Periodic Hann window, w[k] = 0.5·(1 − cos(2πk/N)). [OWN CHOICE]:
    /// the STFT window function is unspecified publicly; Hann is the
    /// standard DSP default for overlapped analysis.
    ///
    /// Bit-determining: the window reweights spectral leakage across the FFT
    /// bins before the chroma fold, so a different window (Hamming, Blackman,
    /// Hann², …) changes emitted bits even after per-frame normalization
    /// (normalization removes a common scalar, not the redistribution across
    /// the 12 classes). `hannWindowShape` only checks loose shape invariants
    /// (endpoints, symmetry, monotonic first half) that OTHER windows also
    /// satisfy, so the identity pin `windowPinned` locks the exact Hann shape
    /// — the twin of `referenceTuningFrequency`/`silenceNormEpsilon`.
    static func hannWindow(length: Int) -> [Float] {
        guard length > 0 else { return [] }
        var window = [Float](repeating: 0, count: length)
        for k in 0..<length {
            window[k] = Float(0.5 * (1.0 - cos(2.0 * Double.pi * Double(k) / Double(length))))
        }
        return window
    }

    /// Power spectrum (magnitude squared) of one frame after applying the
    /// periodic Hann window. Output count = frameSize/2 + 1 (DC..Nyquist).
    /// Uses energy rather than magnitude ([OWN CHOICE]; energy folding per
    /// source 4's energy-band framing). Absolute scale is irrelevant
    /// downstream (per-frame normalization follows).
    static func powerSpectrum(frame: [Float]) -> [Float] {
        precondition(frame.count == frameSize, "frame must be exactly \(frameSize) samples")
        guard let setup = vDSP_create_fftsetup(log2FrameSize, FFTRadix(kFFTRadix2)) else {
            preconditionFailure("vDSP FFT setup failed")
        }
        defer { vDSP_destroy_fftsetup(setup) }
        var power = [Float](repeating: 0, count: frameSize / 2 + 1)
        powerSpectrum(
            frame: frame[frame.startIndex...],
            window: hannWindow(length: frameSize),
            setup: setup,
            into: &power)
        return power
    }

    /// Core windowed-FFT-power routine shared by the single-frame API and
    /// the bulk chromagram path (which reuses one FFT setup across frames).
    private static func powerSpectrum(
        frame: ArraySlice<Float>,
        window: [Float],
        setup: FFTSetup,
        into power: inout [Float]
    ) {
        let n = frameSize
        let half = n / 2
        var windowed = [Float](repeating: 0, count: n)
        let multiplied: Void? = frame.withContiguousStorageIfAvailable { framePtr in
            vDSP_vmul(framePtr.baseAddress!, 1, window, 1, &windowed, 1, vDSP_Length(n))
        }
        if multiplied == nil {
            // Defensive fallback; [Float] slices are always contiguous.
            let copied = Array(frame)
            vDSP_vmul(copied, 1, window, 1, &windowed, 1, vDSP_Length(n))
        }

        var realPart = [Float](repeating: 0, count: half)
        var imagPart = [Float](repeating: 0, count: half)
        realPart.withUnsafeMutableBufferPointer { realPtr in
            imagPart.withUnsafeMutableBufferPointer { imagPtr in
                var split = DSPSplitComplex(
                    realp: realPtr.baseAddress!, imagp: imagPtr.baseAddress!)
                windowed.withUnsafeBufferPointer { windowedPtr in
                    windowedPtr.baseAddress!.withMemoryRebound(
                        to: DSPComplex.self, capacity: half
                    ) { complexPtr in
                        vDSP_ctoz(complexPtr, 2, &split, 1, vDSP_Length(half))
                    }
                }
                vDSP_fft_zrip(setup, &split, 1, log2FrameSize, FFTDirection(kFFTDirection_Forward))
                // Packed real FFT: realp[0] holds DC, imagp[0] holds
                // Nyquist. zvmags fills bins 0..<half; DC and Nyquist are
                // then fixed up explicitly. Absolute scaling (vDSP's factor
                // of 2) is irrelevant — normalization follows downstream.
                power.withUnsafeMutableBufferPointer { powerPtr in
                    vDSP_zvmags(&split, 1, powerPtr.baseAddress!, 1, vDSP_Length(half))
                }
                power[0] = realPtr[0] * realPtr[0]
                power[half] = imagPtr[0] * imagPtr[0]
            }
        }
    }

    // MARK: Stage 2 — chroma folding

    /// Pitch class (0 = A, …, 11 = G#) for a frequency, or nil when outside
    /// [minChromaFrequency, maxChromaFrequency]. Standard pitch-class math
    /// (source 5): class = round(N·log2(f/440)) mod N, where N is the number
    /// of pitch classes per octave. In 12-TET that count IS `chromaBinCount`
    /// (each output bin is exactly one pitch class), so BOTH the octave
    /// divisor and the wrap-around modulus are `chromaBinCount` — never bare
    /// `12` literals.
    ///
    /// This coupling is bit-determining and load-bearing. Perturbing the
    /// divisor alone (e.g. 12.0 → 12.02) leaves `chromaClassMath`'s spot tones
    /// green while reassigning ~46 in-range FFT bins to adjacent classes;
    /// changing the modulus pair (12 → 11) rewrites the whole fold, also with
    /// the spot tones green — exactly the silent-staleness hole the identity
    /// pin exists to close (the twin of `referenceTuningFrequency`). Tying
    /// both to the pinned `chromaBinCount` makes them non-independently
    /// settable: any change fails `pipelineConstantsPinned`.
    static func chromaClass(forFrequency frequency: Double) -> Int? {
        guard frequency >= minChromaFrequency, frequency <= maxChromaFrequency else {
            return nil
        }
        let steps = Int((Double(chromaBinCount) * log2(frequency / referenceTuningFrequency)).rounded())
        return ((steps % chromaBinCount) + chromaBinCount) % chromaBinCount
    }

    /// FFT bin -> chroma class lookup for the 4096-point spectrum at
    /// 11025 Hz (nil = bin outside the chroma fold range). Deterministic,
    /// computed once.
    private static let binChromaClasses: [Int?] = (0...(frameSize / 2)).map { bin in
        chromaClass(
            forFrequency: Double(bin) * Double(requiredSampleRate) / Double(frameSize))
    }

    /// Fold one power spectrum into 12 unnormalized chroma-bin energies.
    static func chromaVector(powerSpectrum: [Float]) -> [Float] {
        precondition(powerSpectrum.count == frameSize / 2 + 1,
                     "power spectrum must have \(frameSize / 2 + 1) bins")
        var chroma = [Float](repeating: 0, count: chromaBinCount)
        for (bin, energy) in powerSpectrum.enumerated() {
            if let pitchClass = binChromaClasses[bin] {
                chroma[pitchClass] += energy
            }
        }
        return chroma
    }

    // MARK: Stage 3 — chroma image (normalize + smooth)

    /// The full chroma image: one 12-vector per STFT frame, in frame order.
    ///
    /// "After some filtering and normalization" (source 1) is unspecified;
    /// [OWN CHOICE]: (1) each frame's fold is normalized to unit Euclidean
    /// norm (loudness invariance; frames at/below `silenceNormEpsilon` — or
    /// with a non-finite norm from corrupt input — stay all zero), then
    /// (2) a 3-frame centered moving average per bin (edges
    /// average only the frames that exist) smooths codec/alignment jitter —
    /// favoring the differ's need for frequent EXACT 32-bit matches.
    static func chromagram(samples: [Float]) -> [[Float]] {
        guard samples.count >= frameSize else { return [] }
        let frameCount = (samples.count - frameSize) / hopSize + 1
        guard let setup = vDSP_create_fftsetup(log2FrameSize, FFTRadix(kFFTRadix2)) else {
            preconditionFailure("vDSP FFT setup failed")
        }
        defer { vDSP_destroy_fftsetup(setup) }
        let window = hannWindow(length: frameSize)

        var raw: [[Float]] = []
        raw.reserveCapacity(frameCount)
        var power = [Float](repeating: 0, count: frameSize / 2 + 1)
        for frameIndex in 0..<frameCount {
            let start = frameIndex * hopSize
            powerSpectrum(
                frame: samples[start..<(start + frameSize)],
                window: window,
                setup: setup,
                into: &power)
            var chroma = chromaVector(powerSpectrum: power)
            let norm = sqrt(chroma.reduce(Float(0)) { $0 + $1 * $1 })
            // Non-finite norms (NaN from corrupt samples; Inf/overflow from
            // pathological amplitudes) take the zeroing branch alongside
            // silence: a NaN norm already fails `>`, and Inf must not reach
            // the division (Inf/Inf = NaN, which the integral image's
            // prefix sums would then propagate into EVERY later window,
            // silently zero-quantizing the rest of the episode). One bad
            // sample stays local — only the frames whose STFT span touches
            // it (±1 smoothing frame) are affected.
            if norm.isFinite, norm > silenceNormEpsilon {
                for bin in 0..<chromaBinCount { chroma[bin] /= norm }
            } else {
                for bin in 0..<chromaBinCount { chroma[bin] = 0 }
            }
            raw.append(chroma)
        }

        return smoothedChromagram(raw)
    }

    /// 3-frame centered moving average per bin; edges shrink to the frames
    /// that exist (frame 0 averages frames 0-1, the last frame averages the
    /// final two), so a steady signal keeps its (unit-norm) frames
    /// unchanged. [OWN CHOICE] — see `chromagram(samples:)`.
    static func smoothedChromagram(_ raw: [[Float]]) -> [[Float]] {
        guard raw.count > 1 else { return raw }
        var smoothed = raw
        for frameIndex in raw.indices {
            let lo = max(0, frameIndex - 1)
            let hi = min(raw.count - 1, frameIndex + 1)
            let span = Float(hi - lo + 1)
            for bin in 0..<chromaBinCount {
                var sum: Float = 0
                for j in lo...hi { sum += raw[j][bin] }
                smoothed[frameIndex][bin] = sum / span
            }
        }
        return smoothed
    }

    // MARK: Stage 4 — filter bank

    /// The fixed 16-filter bank. Placements/scales are [OWN CHOICE]
    /// (chromaprint's actual selection is unpublished), spanning all six
    /// arrangements, all with full-window (16-frame) time extents per
    /// source 2's guidance that large time extents are most robust to
    /// noise/codec distortion. Thresholds are hard-coded from a one-off
    /// calibration on staged corpus podcast audio (response-distribution
    /// quartiles); see docs/xsdz26-fingerprinter-validation.md.
    static let filters: [ChromaFilter] = calibratedFilterBank

    /// Haar-like response of one filter over a 16-frame window
    /// (window[frame][chromaBin], window.count == windowFrameCount).
    /// Identical summation semantics to the integral-image bulk path.
    static func filterResponse(_ filter: ChromaFilter, window: [[Float]]) -> Float {
        precondition(window.count == windowFrameCount,
                     "window must be exactly \(windowFrameCount) frames")
        let integral = IntegralChroma(chromagram: window)
        return Float(response(of: filter, in: integral, atFrame: 0))
    }

    /// Quantize a response to a 2-bit Gray-coded value. Level = number of
    /// thresholds STRICTLY below the response; Gray code 0,1,3,2 (00, 01,
    /// 11, 10) so adjacent levels differ by exactly one bit (source 1).
    static func quantize(response: Float, t1: Float, t2: Float, t3: Float) -> UInt32 {
        let level = (response > t1 ? 1 : 0)
            + (response > t2 ? 1 : 0)
            + (response > t3 ? 1 : 0)
        return grayCodes[level]
    }

    /// Gray code by quantization level: 00, 01, 11, 10.
    private static let grayCodes: [UInt32] = [0b00, 0b01, 0b11, 0b10]

    /// One packed 32-bit subfingerprint for a 16-frame window: filter i's
    /// 2 Gray bits occupy bits (2i+1, 2i), filter 0 least significant.
    /// [OWN CHOICE] packing order.
    static func subfingerprint(window: [[Float]]) -> UInt32 {
        precondition(window.count == windowFrameCount,
                     "window must be exactly \(windowFrameCount) frames")
        let integral = IntegralChroma(chromagram: window)
        return subfingerprint(in: integral, atFrame: 0)
    }

    /// The full subfingerprint stream for a chroma image (integral-image
    /// accelerated; equal to `subfingerprint(window:)` applied at each
    /// position up to floating-point association — the two paths build
    /// their Double prefix sums in different orders, so a response within
    /// ~1 Float ulp of a threshold could in principle quantize differently.
    /// Determinism holds exactly per path; production only uses this path).
    static func subfingerprints(chromagram: [[Float]]) -> [UInt32] {
        guard chromagram.count >= windowFrameCount else { return [] }
        let integral = IntegralChroma(chromagram: chromagram)
        let count = chromagram.count - windowFrameCount + 1
        var stream = [UInt32]()
        stream.reserveCapacity(count)
        for position in 0..<count {
            stream.append(subfingerprint(in: integral, atFrame: position))
        }
        return stream
    }

    // MARK: Filter evaluation internals

    /// Summed-area table over a chroma image, accumulated in Double so
    /// rectangle differences stay far from quantization-flipping error
    /// even over hour-long inputs. [OWN CHOICE] implementation detail.
    private struct IntegralChroma {
        /// (frameCount+1) x (chromaBinCount+1), row-major;
        /// value[(f)*(cols)+(b)] = sum over frames < f, bins < b.
        private let values: [Double]
        private let cols = ChromaFingerprinter.chromaBinCount + 1

        init(chromagram: [[Float]]) {
            let rows = chromagram.count + 1
            var table = [Double](repeating: 0, count: rows * cols)
            for (frameIndex, frame) in chromagram.enumerated() {
                precondition(frame.count == ChromaFingerprinter.chromaBinCount,
                             "chroma frame must have \(ChromaFingerprinter.chromaBinCount) bins")
                var rowSum = 0.0
                let rowBase = (frameIndex + 1) * cols
                let prevBase = frameIndex * cols
                for bin in 0..<ChromaFingerprinter.chromaBinCount {
                    rowSum += Double(frame[bin])
                    table[rowBase + bin + 1] = table[prevBase + bin + 1] + rowSum
                }
            }
            values = table
        }

        /// Sum over frames[frameRange] x bins[binRange].
        func sum(frames: Range<Int>, bins: Range<Int>) -> Double {
            let a = values[frames.upperBound * cols + bins.upperBound]
            let b = values[frames.upperBound * cols + bins.lowerBound]
            let c = values[frames.lowerBound * cols + bins.upperBound]
            let d = values[frames.lowerBound * cols + bins.lowerBound]
            return a - b - c + d
        }
    }

    /// Filter response with the window anchored at chroma frame `origin`.
    private static func response(
        of filter: ChromaFilter,
        in integral: IntegralChroma,
        atFrame origin: Int
    ) -> Double {
        let x0 = origin + filter.timeStart
        let x1 = x0 + filter.timeWidth
        let y0 = filter.chromaStart
        let y1 = y0 + filter.chromaHeight
        switch filter.kind {
        case .totalEnergy:
            return integral.sum(frames: x0..<x1, bins: y0..<y1)
        case .chromaHalves:
            let mid = y0 + filter.chromaHeight / 2
            return integral.sum(frames: x0..<x1, bins: y0..<mid)
                - integral.sum(frames: x0..<x1, bins: mid..<y1)
        case .timeHalves:
            let mid = x0 + filter.timeWidth / 2
            return integral.sum(frames: x0..<mid, bins: y0..<y1)
                - integral.sum(frames: mid..<x1, bins: y0..<y1)
        case .quadrants:
            let midX = x0 + filter.timeWidth / 2
            let midY = y0 + filter.chromaHeight / 2
            return integral.sum(frames: x0..<midX, bins: y0..<midY)
                + integral.sum(frames: midX..<x1, bins: midY..<y1)
                - integral.sum(frames: x0..<midX, bins: midY..<y1)
                - integral.sum(frames: midX..<x1, bins: y0..<midY)
        case .chromaCenterFlanks:
            let flank = filter.chromaHeight / 4
            return integral.sum(frames: x0..<x1, bins: (y0 + flank)..<(y1 - flank))
                - integral.sum(frames: x0..<x1, bins: y0..<(y0 + flank))
                - integral.sum(frames: x0..<x1, bins: (y1 - flank)..<y1)
        case .timeCenterFlanks:
            let flank = filter.timeWidth / 4
            return integral.sum(frames: (x0 + flank)..<(x1 - flank), bins: y0..<y1)
                - integral.sum(frames: x0..<(x0 + flank), bins: y0..<y1)
                - integral.sum(frames: (x1 - flank)..<x1, bins: y0..<y1)
        }
    }

    /// Pack the 16 quantized filter responses at one window position.
    private static func subfingerprint(in integral: IntegralChroma, atFrame origin: Int) -> UInt32 {
        var packed: UInt32 = 0
        for (index, filter) in filters.enumerated() {
            let code = quantize(
                response: Float(response(of: filter, in: integral, atFrame: origin)),
                t1: filter.t1, t2: filter.t2, t3: filter.t3)
            packed |= code << (2 * index)
        }
        return packed
    }

    // MARK: Calibrated filter bank constants

    /// [OWN CHOICE] Fixed 16-filter bank. Geometry: all filters span the
    /// full 16-frame window (source 2: large time extents are the robust
    /// ones); chroma bands cover full/half/third/quarter splits of the 12
    /// pitch classes across all six arrangements. Thresholds are the
    /// 25/50/75th percentiles of each filter's response distribution
    /// measured once over staged corpus podcast audio (6 shows x 5 min,
    /// diverse: conversational / produced / narration — see
    /// docs/xsdz26-fingerprinter-validation.md), then HARD-CODED here so
    /// the fingerprinter is fully deterministic with no runtime calibration.
    /// Thresholds below were produced by the scratchpad calibration harness
    /// (2026-07-07) as the 25/50/75th percentiles of each filter's response
    /// over 14,436 sliding windows drawn from 6 staged corpus episodes
    /// (conan, morbid, 99pi, planet-money, rest-is-history, fresh-air;
    /// 5 minutes each starting at t=60s) and are FIXED — the fingerprinter
    /// never calibrates at runtime.
    private static let calibratedFilterBank: [ChromaFilter] = [
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
}
