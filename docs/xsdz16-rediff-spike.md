# xsdz.16 — On-Device Rediff Spike: double-fetch DAI width oracle

**Bead:** playhead-xsdz.16 (P1 spike, lead width-oracle candidate after the xsdz.24 calibration verdict)
**Date:** 2026-07-06
**Status:** COMPLETE

## Recommendation

**GO** — promote rediff double-fetch to the production width oracle for DAI
slots, with the conditions in §7. The decisive numbers:

- **Rotation coverage:** 36/41 corpus episodes (88%) rotate at a weeks gap
  (8/10 on the probed subset); rotation is 0/10 seconds apart and 2/10 an hour
  apart — so the oracle works for the dominant listening pattern (episodes
  played ≥ a day after download) and the re-fetch policy must wait ≥ ~24 h.
  The days-gap rate (the exact production scenario) is bracketed 20–88% and
  is the one number still to measure in dogfood.
- **Boundary quality:** exact-by-construction. On synthetic ground-truth
  splices the differ recovers boundaries within ~1 s of the true splice
  (119.1 s / 151.0 s recovered vs 120 s / 150 s true, at 0.125 s fingerprint
  granularity) — against the acoustic splice channel's 21% recall @ ±8 s
  (xsdz.24), this is a different class of oracle.
- **Feasibility:** Range requests work on 10/10 corpus CDNs; full re-download
  costs ≈54 MB/episode ≈ 1.1 GB per typical library-week — acceptable under a
  WiFi-and-charging-only policy.
- **Prototype:** the pure-Swift differ matches the python reference EXACTLY
  (fingerprint-index level) on all 5 checked-in fixtures, 3 freshly fetched
  full-episode real pairs, and 5,100 differential-fuzz cases across three
  independent harnesses; it emits slots natively in the played copy's
  timeline, which closes the ±180 s drift problem from xsdz.24.
- **Scope honesty:** DAI-only by construction (baked-in host reads fall
  through to the existing pipeline); order-swapped and equal-length creative
  rotations are invisible (high precision, incomplete recall); one CDN
  (Megaphone/mgln.ai) re-encodes per stitch and must be caught by the
  aligned-fraction guard the prototype already computes.

## 1. What was built and measured

1. **Swift prototype differ** — a pure, test-target-only Swift port of the
   `scripts/l2f-dai-rediff.py` fingerprint alignment algorithm, extended to emit
   slots in the **played copy's (A-side) timeline** (the production design
   constraint; the python reference only emits fresh-copy/B-side gaps).
   `PlayheadTests/Services/AdDetection/RediffSpike/`. Nothing in production
   invokes it.
2. **CDN capability matrix** — HEAD + Range probes against all 10 distinct
   enclosure hosts in the corpus manifest.
3. **Rotation measurements** — 10 episodes (one per CDN host): double-fetch
   back-to-back (seconds gap), a third fetch ~1 hour later, and each fresh copy
   diffed against the staged May snapshot (~5-week gap). Audio deleted
   immediately after fingerprinting; only fingerprints/JSON kept.
4. **Bandwidth math** — costed from real enclosure sizes + the CDN matrix.
5. **Oracle integration sketch** — how rediff slots feed the merged (flag-OFF)
   xsdz.15 SpliceSlot machinery.

## 2. Swift prototype differ

**Code (test-target-only; nothing in production invokes it):**
- `PlayheadTests/Services/AdDetection/RediffSpike/RediffPrototype.swift` —
  exact port of the reference's `find_runs` / `merge_runs` / `gaps_in_b`
  (inverted-index anchoring, constant-offset run extension with Hamming
  tolerance 2, noise-gap merging), plus two additions the reference lacks:
  - **`gapsInA` — the design-critical piece.** The device diffs its OWN
    played copy (A) against the re-fetch (B) and emits slots in the PLAYED
    timeline as the complement of the *union* of run-covered A-intervals
    (runs sorted by B can overlap in A, so the naive B-side gap logic is
    provably wrong there — pinned by discrimination tests). Fresh-download
    coordinate drift (±180 s per xsdz.24) never leaks into the output.
  - **`Result.alignedFractionB`** — the re-encode guard input for §6/§7.
- `PlayheadTests/Services/AdDetection/RediffSpike/RediffPrototypeTests.swift`
  + `RediffSpikeFixtures.swift` — 37 tests in two suites: 30 synthetic
  exact-index tests (SplitMix64-seeded splice/rotation/drift/noise/boundary
  constructions with hand-derivable expectations) and fixture-parity tests
  over 5 checked-in fixtures (`TestFixtures/RediffSpike/*.json`) generated
  from real corpus audio + the actual python reference.

**Accuracy vs ground truth (synthetic real-audio splices, exact truth by
construction):** inserted-ad boundaries recovered at 119.07–151.02 s vs true
120–150 s (insert-in-a/-b), and rotation (30 s ad in A vs 20 s ad in B at the
same content position) recovered as slotA 119.2–153.1 s / slotB 119.2–143.1 s
— all within ~1–3 s of truth at 0.125 s/fingerprint granularity, in the
correct (played) timeline.

**Agreement with the python reference:** EXACT at fingerprint-index level
(merged runs, A-slots, B-slots, min-gap thresholds) on all 5 fixtures, on 3
freshly fetched full-episode real pairs (planet-money, smartless, up-first;
May snapshot vs 2026-07-06 fetch; aligned fractions 0.84–0.91, 3–4 slots
each), and on 5,100 randomized differential-fuzz cases (4,000 + 600
hostile-alphabet + 500 splice-composite; three independently written
harnesses across review rounds). Zero mismatches anywhere.

**Verification history (iterative-implementation workflow, cap 20):**
TDD implementation (genuine red → green), then fresh-reviewer fix rounds:
R1 fixed 4 (test-discrimination holes incl. a naive-`gapsInA` blind spot),
R2 fixed 5 (a surviving mutant killed, semantic-limitation pins,
`alignedFractionB`, precondition hardening), one aborted round (reviewer
crashed mid-mutation-test; damage detected via fixture parity and reverted;
in-place mutation testing banned thereafter), R3 fixed 2 (doc-contract
accuracy), R4 CLEAN, R5 CLEAN → two-consecutive-clean. Focused suites
37/37 green; mutation testing confirmed the suite kills tampering with the
anchor choice, backward-extension bound, union-coalescing, and Hamming
tolerance.

## 3. CDN capability matrix

Probed 2026-07-06 (UTC ~16:57): one representative enclosure URL per distinct
host, via HEAD (redirect-following), `Range: bytes=0-1023`, a mid-file range,
and a second HEAD ~2s later. (The raw probe JSON lived in the session
scratchpad, which was swept by an external cleanup mid-spike; the tables here
are the surviving record. Re-running the probes is ~2 minutes if ever needed.)

| Host | Redirect hops | Accept-Ranges | Range GET | ETag | Last-Modified | Origin/edge server |
|---|---|---|---|---|---|---|
| clrtpod.com | 9 | bytes | 206 | no | no | CloudFront |
| dts.podtrac.com | 2 | bytes | 206 | no | yes | AIS Streaming Server |
| mgln.ai | 5 | bytes | 206 | no | no | envoy (Megaphone) |
| pdst.fm | 7 | bytes | 206 | no | yes | (Podsights chain) |
| podtrac.com | 4 | bytes | 206 | no | yes | (Omny chain) |
| prfx.byspotify.com | 3 | bytes | 206 | no | yes | AIS Streaming Server |
| pscrb.fm | 3 | bytes | 206 | no | no | Fastly (art19) |
| rss.art19.com | 1 | bytes | 206 | no | no | Fastly |
| sphinx.acast.com | 0 | HEAD broken; GET honors Range | 206 | yes (GET only) | no | Acast |
| tracking.swap.fm | 4 | bytes | 206 | no | yes | AIS Streaming Server |

Key facts:

- **Range requests work on 10/10 hosts** (206 with correct `Content-Range`),
  even through 4–9-hop tracking-redirect chains.
- **ETags are effectively absent** (only Acast returns one, and only on GET).
  Conditional-GET-based "did it change?" is NOT viable; `Content-Length`
  comparison and content hashing are the only change signals.
- **Content-Length is not a reliable change signal either**: podtrac returned
  two different totals for the same episode seconds apart (HEAD said 76,856,108;
  the immediately-following ranged GET said 84,496,614) — evidence of
  per-request stitching/variant selection at the edge. Acast's HEAD is
  outright broken (returns `content-length: 2`, no Accept-Ranges).
- **Every fetched size differed from the May manifest size** on all 10 hosts —
  consistent with widespread DAI re-stitching over a weeks-long gap.
- **May-era enclosure URLs (including art19's signed `rss_browser` URLs) still
  resolved 5+ weeks later** on all 10 hosts. URL expiry was not observed, but
  cannot be assumed in general (see failure modes).

## 4. Rotation measurements

Protocol: 10 episodes, one per distinct CDN host. Each enclosure fetched twice
back-to-back (~10 s apart), once more ~65 min later, and each fresh copy
diffed (chromaprint fingerprint alignment, the python reference algorithm)
against the staged May snapshot (~5-week gap). Audio deleted immediately after
fingerprinting (peak transient disk use: one episode, ≤100 MB).

**Rotation rate by re-fetch gap (2026-07-06):**

| Gap | Rotated | Detail |
|---|---|---|
| ~10 seconds (back-to-back) | **0 / 10** | All byte-identical (sha256 equal) — CDNs serve a cached stitch |
| ~65 minutes | **2 / 10** | mgln.ai (techcrunch) + podtrac (nikki-glaser) re-stitched; other 8 byte-identical |
| ~5 weeks (May snapshot → now) | **8 / 10** | Clean multi-slot structure on 7; 1 alignment failure (see below); 2 non-rotators (charlie-kirk, last-week-in-ai: sha changed but audio 100% aligned → metadata-only) |
| ~5 weeks, full 41-episode corpus (tier-a run, 2026-06-03) | **36 / 41 (88%)** | 198 slots, median 49 s — corroborates the subset |

Reading: rotation is effectively **zero within a CDN cache window**, begins
within the hour on the most aggressive stitchers, and approaches ~85–90% at
weeks. The production scenario (re-fetch days after first download) is
bracketed between 20% and ~88%; the days-gap point could not be measured
inside one session and is the single biggest open number (follow-up: dogfood
measurement with a 2–3-day gap).

**Slot structure (weeks-gap, played/A timeline):** e.g. pod-save-america
4 slots (61–143 s, 2445–2527 s, 3418–3500 s, 5039–5150 s); smartless 4 slots
including a 0–90 s preroll; planet-money 4 slots. Median slot 82 s on the
subset (n=37), 49 s on the full tier-a corpus (n=198) — consistent with
1–3 stacked DAI creatives per break.

**Anomalies observed (feed into §7 failure modes):**
- **techcrunch / mgln.ai (Megaphone):** fresh copies fail to fingerprint-align
  with the May snapshot almost everywhere (5.6 s aligned of a 280 s episode)
  and even hour-apart fresh copies only align 67 s — the pipeline re-encodes
  or re-normalizes audio per stitch. Rediff must detect low aligned-fraction
  and discard.
- **nikki-glaser / podtrac (Omny):** hour-gap rotation produced 18 A-side
  gap fragments, several of 5–10 s — fingerprint dropouts around splice
  points fragment slot edges; the oracle consumer should merge nearby
  fragments before use.
- **casefile / Acast:** one 29-minute "slot" (306–2057 s) at weeks gap —
  alignment breakdown over a long stretch, not a plausible ad. Needs a
  slot-duration sanity cap.

## 5. Bandwidth math

Measured enclosure sizes (10-episode subset): 11.4–99.4 MB, median 44.8 MB,
mean 51.9 MB. Full 41-episode manifest: median 56.1 MB, mean 54.1 MB, total
2.22 GB. Back-to-back downloads completed in 0.6–5.7 s each on a residential
connection — CDN throughput is not a constraint; the cost is pure bytes.

**Strategy A — full re-download (the one that works):** one rediff = one extra
full episode download ≈ +100% of the episode's original delivery cost.
- Per episode: ~54 MB (mean).
- Typical library-week (≈20 new episodes across ~10 shows): **≈1.1 GB/week**
  of re-fetch traffic if every episode is rediffed once.
- Restricting rediff to episodes the user is likely to play (Playhead already
  tracks per-show listening) cuts this roughly in half for typical libraries.

**Strategy B — ranged sampling (rejected for boundary extraction):** DAI
insertion *shifts every downstream byte*, so fixed-offset chunk hashes cannot
localize boundaries; fingerprint alignment needs the full audio stream anyway.
Ranged sampling is only useful as a cheap change pre-check.

**Strategy C — hybrid pre-check + full download (recommended):** fetch head
64 KB + tail 64 KB (~128 KB) and compare against the same ranges hashed at
first-download time. Identical head+tail+length → very likely unchanged →
skip tonight, retry later (~10–20% of episodes at weeks gap; both non-rotators
in our sample, last-week-in-ai and charlie-kirk, would be caught). Any
difference → full re-download. Saves the full fetch only on non-rotating
feeds, but those are exactly the feeds where re-fetching nightly forever would
otherwise be pure waste. Caveat: a mid-file-only rotation with identical
head/tail/length would be missed; sizes changed in 10/10 observed rotations,
so the risk is low, and a periodic unconditional full fetch (e.g. every 3rd
attempt) bounds it.

**Policy implication:** rediff traffic must be **WiFi-and-charging only**
(same posture as the existing overnight analysis pipeline). ~1 GB/week over
WiFi is acceptable for a nightly background window; it would be unacceptable
on cellular. No always-on network listener is needed — a scheduled BGTask
that runs when constraints are satisfied is sufficient.

## 6. Oracle integration sketch (design only — no code in this spike)

The merged flag-OFF xsdz.15 machinery is deliberately **oracle-agnostic**; the
rediff oracle replaces only the *acoustic pair-finding* step.

**What exists today** (all in `Playhead/Services/AdDetection/`):
- `SpliceSlotResolver.resolve(core:vetoedRanges:breaks:episodeWindows:) -> SpliceSlot?`
  — the acoustic pair-finder: picks a start/end edge pair from
  `AcousticBreak`s scored by `AudioForensicsBoundaryDetector`. **This is the
  step rediff replaces.**
- `SpliceSlotCandidate { mintedInterval, slot, slotIntersectsAtoms, coreBankMatch, slotBankMatch }`
  → `SpliceSlotDispositionEngine.computeDispositions(_:)` (passes 2–4) →
  `SpliceSlotRewriter.apply(decodedSpans:dispositions:atomEvidence:)` (pass 5).
  All pure; **reusable unchanged** — the engine never asks where a `SpliceSlot`
  came from.
- Gating: `Configuration.spliceSlotOwnershipEnabled` / `spliceSlotShadowEnabled`
  (both default false); shadow rows (`SpliceSlotShadowRow`) and the
  `AnchorRef.spliceSlot` provenance case persisted via `anchorProvenanceJSON`.

**Proposed shape:**

1. **`RediffSlotStore` (new):** per-episode record
   `{ playedCopySha256, fingerprints (compact binary, ~115 KB/hour), rediffSlots: [start, end, confidence], alignedFraction, fetchedAt }`
   persisted in `AnalysisStore`. Fingerprints of the *played* copy are computed
   at (or shortly after) download and are the only thing retained; fresh-fetch
   audio is fingerprinted streaming and discarded.
2. **Re-fetch job (new):** BGTask, WiFi+charging, earliest ~24 h after first
   download (short-gap measurement says immediate re-fetch is useless: 0/10
   rotated back-to-back). Pre-check per Strategy C; on change, stream-download,
   fingerprint, run the differ (played copy = A), store `rediffSlots` in the
   played timeline, delete audio. If unchanged, exponential backoff (1d → 2d →
   4d), give up after ~3 attempts (baked-in-only shows never rotate).
3. **Slot-pass integration:** in `computeSpliceSlotPass`, when episode has
   rediff slots with `alignedFraction ≥ 0.5` (re-encode guard, see §7):
   for each minted interval, prefer a rediff slot overlapping the core over
   `SpliceSlotResolver`'s acoustic pair; synthesize the `SpliceSlot` from the
   rediff span (edges carry the rediff flank-confidence as `stepScore`).
   Acoustic resolver remains the fallback for episodes/intervals without
   rediff coverage. Candidates flow into the existing disposition engine
   untouched — bank vetoes, atom-intersection, greedy-collision all still
   apply. Rediff is **authoritative on width, never on existence**: it
   proposes spans; the disposition + decision layers still decide.
4. **Provenance:** add `AnchorRef.rediffSlot` (bare case, mirroring
   `.spliceSlot`) so persisted spans record which oracle set their width;
   shadow mode (`spliceSlotShadowEnabled`) works as-is for a
   measure-before-enable rollout, with `SpliceSlotShadowRow.sourceSlot`
   distinguishing rediff-sourced slots.
5. **Timing semantics / played-timeline alignment:** the device diffs **its own
   played copy** against the re-fetch, and the differ emits gaps-in-A — slots
   are natively in the timeline the user hears. Fresh-copy coordinates (which
   drift up to ±180 s; xsdz.24) never leak out of the differ. If the user
   plays the episode before any successful rediff, the existing pipeline output
   stands; when rediff lands later it can retro-correct spans for banners,
   eval, and future replays (same re-persist path pass 5 already uses).
6. **Non-rotating (baked-in) ads:** rediff finds nothing (sha-identical or
   full-coverage alignment) → record "no rediff signal" and fall through to
   existing acoustic/FM behavior. Host-read/baked-in ads are out of scope for
   this oracle by construction.

## 7. Failure modes

| Failure | Observed? | Mitigation |
|---|---|---|
| Enclosure URL expired / feed 404 | Not observed (May URLs still live on 10/10 hosts) | Re-resolve the feed first and use the current enclosure URL for the episode GUID; if gone, skip permanently |
| Re-encoded audio (fingerprints don't align anywhere) | **Yes** — techcrunch/mgln.ai (Megaphone): only 5.6 s aligned of a 280 s episode | Require `alignedFraction ≥ 0.5` before trusting slots; below → discard rediff, fall through. (This exact pair is checked into the fixtures as `real-rotated-pair.json`.) |
| CDN A/B or per-request stitching variance | **Yes** — podtrac served two different Content-Lengths seconds apart | Harmless by design: any B-arm works because slots are expressed in OUR played copy's timeline; variance only means "rotation detected" fires more often |
| Alignment breakdown producing giant slots | **Yes** — casefile produced a 29-min "slot" in a 113-min episode | Sanity cap: slots > ~8 min or slots overlapping < min flank confidence get downgraded to "suspect" and require corroboration from the existing pipeline |
| Metadata-only change (sha differs, audio identical) | **Yes** — last-week-in-ai: sha changed, 100% aligned, zero slots | Already handled: full-coverage alignment yields no slots; record as "no rotation" |
| Partial/truncated download | Not observed | Compare received bytes vs final Content-Range/Content-Length; discard on mismatch |
| HEAD unreliable (Acast `content-length: 2`; podtrac HEAD≠GET) | **Yes** | Never trust HEAD for change detection; use ranged GET samples (Strategy C) |

## 8. Follow-up beads if GO

In dependency order:

1. **On-device fingerprinter (DECISION NEEDED FIRST).** The prototype consumes
   chromaprint fingerprints; `fpcalc` is a macOS binary and cannot ship on
   iOS. Options, per the no-unilateral-swaps rule this is Dan's call:
   (a) link the chromaprint C library (LGPL-2.1+ — dynamic linking obligations
   on iOS need a look), (b) pure-Swift reimplementation of the chromaprint
   pipeline (no new dependency, most work, fully mandate-clean), (c) a
   different fingerprint of our own design (cheapest compute, but forfeits
   fpcalc-compatibility with all corpus tooling and the checked-in fixtures).
   The differ itself is fingerprint-agnostic as long as both sides use the
   same extractor at the same rate.
2. **Played-copy fingerprint capture + `RediffSlotStore`** — fingerprint at
   (or shortly after) download, persist ~115 KB/hour per episode in
   AnalysisStore; store rediff slots + alignedFraction + provenance.
3. **Re-fetch BGTask + policy** — WiFi+charging, ≥24 h after first download,
   Strategy C pre-check (head/tail 64 KB + length), backoff 1d/2d/4d, give up
   after ~3 unchanged attempts; stream-fingerprint the fresh copy, never
   persist its audio.
4. **Slot-pass integration (flag-OFF + shadow first)** — rediff slots replace
   `SpliceSlotResolver`'s acoustic pair-finding when present with
   alignedFraction ≥ 0.5; consumer-side fragment merging (≤ ~3 s joins) and
   slot-duration sanity cap (~8 min); `AnchorRef.rediffSlot` provenance;
   measure via the existing `SpliceSlotShadowRow` path before enabling.
5. **Days-gap rotation dogfood measurement** — instrument the re-fetch job to
   record rotation-vs-gap on the real library; closes the 20–88% bracket and
   tunes the backoff schedule.

## Appendix: method notes

- Reference algorithm: `scripts/l2f-dai-rediff.py` (chromaprint `fpcalc
  -raw`, ~7.93 fingerprints/s). Fixtures were generated by running the actual
  reference functions on real corpus audio (and ffmpeg-spliced variants with
  known ground truth) and embedding the outputs as `pythonReference` blocks.
- All fetches in this spike were sequential with a normal UA; audio was
  deleted immediately after fingerprinting (14 GB free disk constraint).
- Raw measurement JSONs (CDN probes, rotation runs, fingerprint files) were
  lost mid-spike to an external scratchpad cleanup; every number cited here
  was extracted before the loss, and the checked-in fixtures + this document
  are the durable record. The 3-pair real agreement run was redone afterwards
  from fresh fetches (its numbers are post-loss and fully reproducible).
