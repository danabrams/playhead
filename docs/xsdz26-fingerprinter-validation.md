# xsdz.26 — ChromaFingerprinter validation results

**Bead:** playhead-xsdz.26 (clean-room chromaprint-class fingerprinter)
**Date:** 2026-07-07
**Implementation:** `Playhead/Services/AdDetection/Fingerprinting/ChromaFingerprinter.swift`
**Provenance:** clean-room per `docs/xsdz26-cleanroom-notes.md` — implemented
exclusively from the cited published descriptions; the chromaprint C/C++
source (LGPL) was never read, fetched, or consulted. Bit-compatibility with
chromaprint is a non-goal; alignment quality through the rediff differ is
the goal.

## Chosen operating point (pinned by `ChromaRediffOperatingPointPinTests`)

| Knob | Value | Note |
|---|---|---|
| secondsPerFp (both arms) | **1365/11025 ≈ 0.123810 s** | the fingerprinter's exact exposed rate |
| hammingTol | **5** | fpcalc-era default was 2; see below |
| minRunLen | 8 | differ default |
| offsetSlack | 2 | differ default |
| gapDiffSlack | 2 | differ default |
| minAdSeconds | 5.0 | differ default |

**Why hammingTol=5:** a DAI insert is almost never an integer multiple of
the 1365-sample hop, so all post-splice STFT frames in the B arm are
computed on a sub-hop-shifted grid (worst case ~62 ms). Speech chroma
tolerates that; music-heavy content loses a few bits per subfingerprint.
Run *seeding* stays anchor-EXACT in the differ, so raising the extension
tolerance does not loosen seeding. Measured on the harness worst case
(radiolab, insert phase ≈ 0.47 hop): boundary end error 6.5 s @ tol=3,
6.4 s @ tol=4, **2.1 s @ tol=5**, with no false runs and re-encode-identical
alignment at 1.000 across the matrix at tol=5. A 5-frame smoothing variant
at tol=4 was tried and REJECTED: it produced a false run inside the insert
(heavier smoothing = fewer effective bits).

## Boundary geometry (windowBias — integration bead must apply this)

A subfingerprint anchored at frame *i* spans frames *i..i+15*, so reported
gap **starts** carry a systematic early bias while gap **ends** are
unbiased (the first clean window starts at the insert's end). The
correction constant used is `(windowFrameCount-1) * secondsPerFp
≈ 1.857 s`, and it is **empirically centered, not exact geometry**: the
true audio span of a subfingerprint is 15 hops + one 4096-sample STFT
frame (≈ 2.23 s, plus ±1 frame of smoothing bleed), but Hann edge taper
and hammingTol=5 let runs survive partway into contaminated windows, so
measured raw start errors landed at 1.39–1.89 s. Treat the correction as
1.857 s ± ~0.5 s of model error; the validation suite asserts start
boundaries after this correction (tolerance 1.5 s) and end boundaries raw
(tolerance 2.0 s). The integration bead should apply the same correction
with the same error band, not as an exact constant.

## Validation (a): synthetic splice pairs, 3 shows

Construction: 240 s of real content C (from t=300 s of each episode),
B = C with 30 s of a different show (techcrunch daily) spliced in at
exactly 120 s. Ground truth exact by construction. Differ output slotsB
vs truth (macOS harness numbers; the simulator suite asserts the same
bars and is green):

| Show / character | raw startErr | bias-corrected startErr | endErr | alignedFractionB |
|---|---|---|---|---|
| smartless (conversational) | 1.39 s | 0.47 s | 0.19 s | 0.883 |
| radiolab (produced/music) | 1.39 s | 0.47 s | 0.31 s | 0.884 |
| casefile (true-crime narration) | 1.89 s | 0.03 s | 0.31 s | 0.882 |

(alignedFractionB ≈ 0.889 is the theoretical max for a 30 s insert in
270 s of B.) Additional harness probes at insert 45.71 s @ 77.3 s and
15.04 s @ 183.6 s stayed within: corrected startErr ≤ 0.5 s, endErr ≤ 1.05 s
— except the deliberate worst-phase music case (radiolab, phase ≈ 0.47 hop)
at endErr 2.06 s. All within the xsdz.16 spike's 1–3 s fpcalc bar.

## Validation (b): re-encode robustness (Megaphone-class acid test)

Transcode is done IN-TEST via AVFoundation (Process/afconvert is
unavailable in the simulator): 11025 mono → 44100 → **AAC 64 kbps** m4a on
disk → decode → 11025 mono. This includes two resample passes and the AAC
encoder's priming/trim behavior, i.e. harsher than a plain re-encode.

| Case (all 3 shows) | alignedFractionB | slots |
|---|---|---|
| identical content, original vs re-encode | **1.000** (bar ≥ 0.8) | zero phantom slots either side |
| spliced + re-encoded vs original | 0.830–0.884 | insert recovered within the same boundary bars as (a) |

Spliced+re-encoded boundary errors matched the un-re-encoded splice within
≤ 0.13 s on every show — the AAC round trip cost essentially nothing on
top of the splice itself.

## Validation (c): real May-arm vs fresh-arm pairs — DEFERRED

No reusable fresh-B-arm audio or fingerprints exist locally (the xsdz.16
spike deleted its fetched audio), and network fetch campaigns are out of
scope for this bead. Real-pair validation is deferred to the width-oracle
integration bead. Synthetic + re-encode coverage above is the accepted
scope for xsdz.26.

## Performance (PerfGate: `ChromaFingerprinterPerfTests`, serial perf pass)

60-minute episode (rest-history), iPhone 17 Pro simulator on the M-series
dev Mac, Debug test build, quiescent CPU:

```
decode 2.92 s + fingerprint 15.85 s = 18.77 s   (29,059 subfingerprints)
```

Budget: **< 60 s** — passes with 3.2x headroom. Notes:

* The test target is a Debug (-Onone) build; the same code compiled -O in
  the calibration harness fingerprints the same hour in **0.2 s**
  (decode 2.1 s). The Release-build production path has enormous headroom.
* **On-device caveat:** this Mac is substantially faster than an iPhone,
  so 18.77 s does NOT transfer to device directly. The realistic device
  estimate is Release-build: fingerprint-only in the 1–2 s class (0.2 s
  on this Mac at -O, times a several-x device penalty) plus decode.
  Re-measure on a real device in the integration bead before wiring
  background-processing budgets.
* Memory: 60 min of mono Float32 at 11025 Hz is ~159 MB as a single
  `[Float]`; the integration bead should consider chunked fingerprinting
  if that matters on device.

## Design choices ([OWN CHOICE] summary; full rationale in code comments)

* Hop 1365 = floor(4096/3) ("2/3 overlap" published without an exact hop);
  exposed rate is the exact rational 1365/11025.
* Periodic Hann STFT window.
* Energy (magnitude-squared) spectrum folded to chroma.
* Chroma fold range 55 Hz (A1) – 3520 Hz (A7), class = round(12·log2(f/440)) mod 12.
* Chroma "filtering and normalization": per-frame unit-L2 normalization
  (silence stays zero, epsilon 1e-10), then 3-frame centered moving average.
* Filter bank: 16 fixed filters, all full 16-frame time width, spanning
  the five published arrangements + a sixth [OWN CHOICE] `totalEnergy`
  arrangement; band splits at full/half/third/quarter chroma heights.
* Thresholds: per-filter 25/50/75th percentiles of response distributions
  measured ONCE over 6 staged corpus episodes (conan, morbid, 99pi,
  planet-money, rest-is-history, fresh-air; 5 min each from t=60 s;
  14,436 windows), then hard-coded. No runtime calibration.
* Quantization: level = #(thresholds strictly below response); Gray code
  00, 01, 11, 10; filter *i* packed at bits (2i+1, 2i).
* Double-precision integral image for filter sums (Float prefix sums over
  hour-long inputs would put cancellation error within quantization range).
* Input contract: caller supplies mono 11025 Hz `[Float]` (resampling is
  upstream's job; keeps AVFoundation out of the production file).

## Persistence contract (for the xsdz.27 fingerprint store)

* `ChromaFingerprinter.algorithmVersion` (currently **1**) identifies the
  emitted-bit algorithm. A store MUST persist it alongside fingerprints and
  treat any mismatch as stale (re-fingerprint; never Hamming-compare streams
  across versions — cross-version comparison misaligns silently rather than
  failing). `ChromaAlgorithmIdentityPinTests` pins every bit-determining
  constant (pipeline constants + the full filter bank) so an algorithm
  change cannot land without touching the pin and the version together.
* Fingerprints are **same-device artifacts** (Float-ulp caveat under Known
  limitations) — persist and compare on the producing device only; do not
  sync across devices/users without revalidation.
* Non-finite input samples (corrupt decode, overflow) are contained: an
  STFT frame whose chroma norm is non-finite is zeroed like silence, so one
  bad sample affects only the windows whose span touches it instead of
  poisoning the integral-image prefix sums for the rest of the episode
  (pinned by `nonFiniteSampleContainment`).

## Known limitations

* Music-heavy content at worst-case sub-hop splice phase can delay
  post-insert re-seeding by ~2 s (measured; included in the end-boundary
  tolerance). Conversational content is unaffected.
* The differ's inherited semantic limitations (order-swapped fills,
  equal-length rotation) are unchanged — see RediffPrototype header.
* Thresholds were calibrated on 6 English-language podcast episodes; a
  radically different corpus (e.g. pure music feeds) may shift response
  distributions. Quartile thresholds degrade gracefully (bits become
  biased, not broken).
* Cross-device fingerprint exchange is NOT validated: libm (`log2`/`cos`)
  and Float rounding can differ across architectures/OS versions, so a
  response within ~1 Float ulp of a threshold may quantize differently on
  another device. Irrelevant while both arms are fingerprinted on the same
  device (the current design); revisit before any cross-user sharing of
  fingerprints.
