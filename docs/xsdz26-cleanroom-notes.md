# xsdz.26 — Clean-room chromaprint-class fingerprinter: public-source algorithm notes

**Bead:** playhead-xsdz.26
**Provenance discipline (licensing-critical):** everything below was gathered
EXCLUSIVELY from published algorithm descriptions. The chromaprint C/C++
source (LGPL) was NOT read, fetched, or consulted at any point in this bead's
sessions. Where the public descriptions leave a detail unspecified, we make
our own engineering choice and mark it **[OWN CHOICE]**. Bit-compatibility
with chromaprint is explicitly NOT a goal; alignment quality through the
rediff differ is the goal.

## Sources (cite these in the implementation file header)

1. Lukáš Lalinský, "How does Chromaprint work?", oxygene.sk blog, 2011-01-18.
   https://oxygene.sk/2011/01/how-does-chromaprint-work/
   (The chromaprint author's own published description.)
2. Yan Ke, Derek Hoiem, Rahul Sukthankar, "Computer Vision for Music
   Identification", CVPR 2005. https://www.cs.cmu.edu/~rahuls/pub/cvpr2005-rahuls.pdf
   (The filter/classifier design chromaprint's blog description cites.)
3. AcoustID / Chromaprint project page (non-source docs): https://acoustid.org/chromaprint
4. J. Haitsma, T. Kalker, "A Highly Robust Audio Fingerprinting System",
   ISMIR 2002. (Energy-difference subfingerprint bits + bit-error-rate
   matching; described in detail in source 2 §3.1/§6.2.)
5. M. Bartsch, G. Wakefield, "Audio Thumbnailing of Popular Music Using
   Chroma-Based Representations" (chroma = fold spectrum into 12 pitch
   classes; cited by source 1).

## Pipeline as published (source 1, verbatim parameters)

1. Input audio converted to **mono, 11025 Hz**.
2. STFT with **frame size 4096** samples (0.371 s) and **2/3 overlap**
   (=> hop 4096/3 ≈ 1365 samples ≈ 0.1238 s per feature frame).
3. Frequencies transformed into **musical notes; octaves discarded => 12
   chroma bins** ("we are only interested in notes, not octaves").
   "After some filtering and normalization" the chroma image is formed —
   the concrete filtering/normalization is NOT specified publicly.
4. A **sliding 16-frame × 12-bin window** moves over the chroma image. At
   each position **16 filters** are applied; each filter "calculate[s] the
   sum of specific areas of the grayscale subimage and then compare[s] the
   two sums"; there are **six possible area arrangements**.
5. "Every filter has three coefficients associated with it, that say how to
   quantize the real number, so that the final result is an integer between
   0 and 3" — i.e. **3 thresholds => 2 bits, encoded with Gray code**.
6. 16 filters × 2 bits = **one 32-bit subfingerprint per window position**;
   sliding over all positions yields the fingerprint stream. Fingerprints
   are compared via **bit error rate** (Hamming distance).

## Filter family as published (source 2)

- Treat the time-frequency image as a grayscale image; apply
  **Viola-Jones Haar-like rectangle filters**: sum of one region minus sum
  of another, over a window. Source 2 Figure 2 shows 5 arrangements:
  (a) top-minus-bottom halves (difference across frequency/chroma at a time
  interval), (b) left-minus-right halves (difference across time in a band),
  (c) checkerboard/diagonal quadrants (equivalent to Haitsma-Kalker's
  time-frequency derivative), (d) horizontal center-minus-flanks band peak,
  (e) vertical center-minus-flanks time peak. Source 1 says chromaprint's
  class has "six possible area arrangements" — one more than source 2's
  five; the sixth is unspecified publicly. **[OWN CHOICE]** for the sixth
  arrangement (or use only published five).
- Filters vary in band location, bandwidth, and time-width within the
  window; responses are compared to per-filter thresholds; source 2 §3.1:
  thresholds ≈ **median of the filter-response distribution** work well
  ("all thresholds learned by the pairwise boosting are approximately at
  the median ... approximating non-matching error in this manner has
  minimal impact"). Chromaprint's actual selected filters/coefficients/
  thresholds are NOT published => **[OWN CHOICE]**: pick a fixed,
  deterministic set of 16 filters spanning the published arrangements and
  scales; calibrate the 3 quantization thresholds per filter as (approx.)
  the 25/50/75th percentiles of the filter-response distribution measured
  on our own corpus audio, then HARD-CODE the constants (deterministic,
  documented).
- Source 2 design guidance worth honoring: filters with **large time
  extents average out noise/codec distortion** (their learned set favored
  time-widths ≥ 54 of 82 frames at 11.6 ms/frame — i.e. roughly the full
  window); most useful filters "measure the difference in two sets of
  frequency bands at a particular time interval or a peak across frequency
  bands at a particular time interval".

## Unspecified-in-public => [OWN CHOICE] items (document each in code)

- Window function for the STFT (Hamming/Hann — standard DSP choice).
- Frequency range folded into chroma (standard MIR practice: restrict to a
  band that excludes DC/rumble and near-Nyquist; e.g. ~28 Hz–3520 Hz;
  cite as own choice), and the folding formula
  bin = round(12·log2(f/440)) mod 12 (standard pitch-class math, source 5).
- Chroma "filtering and normalization" specifics (e.g. per-frame
  normalization, small temporal smoothing).
- The exact 16 filters, their placements/scales, thresholds, Gray-code bit
  assignment, and bit packing order within the UInt32.
- Fingerprint rate: hop 1365 @ 11025 Hz ≈ 0.1238 s/fp (published overlap) —
  or any nearby rate; the differ takes `secondsPerFp` as a parameter. Keep
  the fingerprinter's rate an exact rational (hop/sampleRate), expose it,
  and PIN the fingerprinter+differ pair by test. The spike/task language
  "0.125 s/fp granularity" is the class of rate, not a mandated constant.

## Differ interop constraints (from the validated spike)

- Differ: `RediffPrototype.rediff(fingerprintA:secondsPerFpA:fingerprintB:secondsPerFpB:hammingTol:minRunLen:offsetSlack:gapDiffSlack:minAdSeconds:)`
  at `PlayheadTests/Services/AdDetection/RediffSpike/RediffPrototype.swift`.
  It is fingerprint-agnostic: `[UInt32]` + seconds-per-fp. Defaults
  (hammingTol 2, minRunLen 8) were tuned on fpcalc-derived fingerprints;
  our fingerprinter may need a different operating point — choose, justify,
  and pin by test.
- Run matching is anchor-EXACT (a run must contain at least one exact
  32-bit match to seed it). Filter/threshold design should therefore make
  exact matches common between two encodes of the same content — favor
  robust (large-area, heavily smoothed) filters.
