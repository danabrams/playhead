# Byte-Substrate Width-Detection Kill Tests — Spike Report

**Beads:** playhead-xsdz.44 (byte-level rediff), playhead-xsdz.43 (codec scars), playhead-xsdz.51 (within-show dedup)
**Date:** 2026-07-17 · Pure Python (stdlib only), offline, no decode, no fpcalc, no xcodebuild.
**Tool:** `scripts/l2f-mp3-forensics.py` (one MP3 frame parser + `scars` / `align` / `dedup` subcommands).
**Data:** 35 gold-labeled A-sides (gold v6 `836b8188…`, 70 full breaks, coverage=PARTIAL / unlabeled=unknown);
3 staged SmartLess `.fresh.mp3` B-sides (copied to scratch before the background re-fetch; sha256 ≠ June ruler's B-sides → they are *new July fetches*);
4 self-downloaded Morbid B-sides (297 MB, 4/4 downloads OK, 4/4 rotated: sha256 ≠ manifest sha).
Secondary ruler: June fpcalc backup `tier-a-rediff.BACKUP-preretain.json` (41 eps).

---

## Verdicts

| Bead | Verdict | Load-bearing number |
|---|---|---|
| **xsdz.44** byte rediff | **GO** | 7/7 pairs monotonic-clean (incl. 4/4 Morbid where fpcalc failed); 11/11 gold breaks matched, \|dEnd\| median **0.02 s**, width coverage **99.3%** (fpcalc on same breaks: 18.9 s / 54.2%) |
| **xsdz.43** codec scars | **PARTIAL** | mdb-reset finds **90.3%** of slot STARTS ±2 s on clean-encoder files, but event precision is **16%** (5.9 FA/h); ends 30.6%; 4 of 5 other scar classes have **0 events in ~35 h** |
| **xsdz.51** within-show dedup | **GO** | **98.5%** of cross-episode shared-run seconds (1998 s over 8 pair-hits) fall inside independently-established ad regions; same-day fetch cohort yields 212–264 s labels/pair |

---

## xsdz.44 — Byte-level rediff (A/B fetch alignment)

**Method.** Anchors = per-frame blake2b hashes unique in both files (frame lattice is
content-defined, so arbitrary byte shifts are handled); anchors grouped by byte delta,
greedily extended to maximal **byte-verified** runs via chunked mmap compares; weighted
longest strictly-monotonic non-overlapping chain; gaps between chained runs = slots
(head / replaced / tail) with byte-exact edges mapped to A-timeline seconds.
`min_run_bytes=65536` (≈4.1 s @128 kbps).

**N.** 7 pairs: SmartLess ×3 (staged July fresh), Morbid ×4 (my July downloads). 6 of 7
A-sides are in gold (smartless-05-11 is ruler-only); 11 gold breaks total in the pair set.

**Results.**

| Pair set | Monotonic clean | Runs found→chained | Slots |
|---|---|---|---|
| SmartLess ×3 | 3/3 | 3→3 each | 4 each |
| Morbid ×4 | **4/4** (fpcalc: 0/4 in 07-17 pilot, `alignment-non-monotonic`) | 3→3, 3→3, 3→3, 5→5 | 4,4,4,6 |

Gold scoring (11 breaks across the 6 gold episodes):

- **Matched 11/11**, all IoU ≥ 0.968 (9 of 11 ≥ 0.985). 0 missed.
- Start deltas: median **+0.29 s**, max 1.36 s (consistent late bias — the byte splice sits
  slightly after Dan's perceptual break start; gold tolerance is ±0.3 s).
- End deltas: median **+0.02 s**, max **0.22 s** — byte-exact for practical purposes.
- Width coverage of matched breaks: **99.3%** (matched-intersection 720.7 s / 725.7 s gold).
- 15 additional byte slots fall in gold-unlabeled space (gold coverage is PARTIAL). None
  collide with a `content_veto`. Each has a corroborating differing-region in the *June*
  fpcalc ruler at the same location, i.e. they rotate across two independent B-fetches
  ~5 weeks apart — they are real ad slots gold simply hasn't labeled.

Same 11 breaks vs the June fpcalc ruler (its slots merged when <5 s apart, generous):
matched 11/11 but |dStart| median **18.89 s** (max 57.1), |dEnd| median **30.31 s**
(max 88.8), width coverage **54.2%**. Byte alignment is ~65× tighter on starts, ~3
orders of magnitude on ends, and closes the width gap 54%→99%.

**Runtime.** Largest pair (103 MB A + 109 MB B) aligns in **1.9 s** wall, no decode.
fpcalc rediff decodes both sides (tens of seconds per episode) and works at ~0.124 s
granularity.

**Surprise / caveat (honest).** smartless-05-11 emits two 15 s mid-roll "slots" where
the June ruler saw 90–170 s differing regions, and SmartLess breaks in gold run 47–113 s.
Interpretation: most of that break's creative was **re-served byte-identically** in the
July fetch (7-week-old A!), so the differ sees only the rotated remainder. This
same-creative blind spot is inherent to ANY two-fetch differ (fpcalc included — it's why
rediff mandates ≥24 h gaps); byte-rediff makes it visible instead of fuzzy. Mitigations:
union over >1 B-fetch, and/or snap-out to the nearest xsdz.43 mdb-reset. 2 of 26 slots in
this set show the signature.

**Verdict: GO.** Byte alignment should replace fpcalc as the rediff truth differ. It
solves the Morbid non-monotonic failure class outright (repeated music beds are not
byte-identical — confirmed), and its edges are gold-tolerance-grade.

---

## xsdz.43 — Codec scars (single-file, day-0)

**Method.** Parser walks all 35 gold A-sides (~35 h audio): side-info `main_data_begin`
per frame; scar classes measured **separately**: `mdb_reset` (mdb==0 with previous frame
mdb>0), `midfile_tag` (Xing/Info/VBRI/LAME at canonical offsets past frame 0),
`midfile_id3`, `resync`, `param_change` (samplerate/version/layer/channel-count; stereo↔
joint-stereo flips excluded as routine LAME behavior), `bitrate_change`. Recall = gold
edges (±2 s, excluding edges <2 s from file boundaries) with a scar nearby; FA = scars
per content-hour outside breaks±2 s.

**N.** 35 episodes, 120 scoreable gold edges (59 starts / 61 ends).

**Results.**

- **Dead classes: `midfile_tag`, `midfile_id3`, `resync`, `param_change`, `bitrate_change`
  = 0 events across all 35 files.** These stitchers emit seamless, header-clean,
  CBR-homogeneous streams: no retained segment headers, no embedded ID3, no sync damage.
  The "structural scar" family is empirically empty on this corpus — kill those.
- `mdb_reset` is the only live class. All-file recall ±2 s: 49.2% (59/120), FA 4.86/h.
- **Base-rate gate (the honesty check that reshapes the verdict):** per-file mdb0 rate is
  bimodal — 21/35 files ≤0.5% (most ≈0.01%), but themove = **100%** (reservoir-free
  encoder: signal nonexistent), unexplained 22–29%, stuff-you-should-know 14%, nikki 9%,
  doac 1.6–1.8%. The scar only exists where the encoder actually uses the bit reservoir.
- On the 21 clean files (mdb0 ≤ 0.5%):
  - **START edges: 28/31 = 90.3% recall ±2 s** (54.8% ±0.5 s; hit distance median 0.38 s).
  - END edges: 11/36 = 30.6% — ad creatives are standalone encodes (first frame mdb=0);
    resumed content is cut from a continuous encode, so slot *exits* rarely scar.
  - Event precision: 40/249 events near any gold edge = **16.1%**; FA 5.93/content-hour.
    (Some FAs are likely legitimate production edits — indistinguishable by this signal.)

**Verdict: PARTIAL.** Not a standalone day-0 width oracle (16% precision, end-blind,
and 14/35 files lack the signal entirely). It IS a cheap, sample-accurate **attention /
snap-to signal for slot starts** on reservoir-using encoders — exactly the
lexical-as-attention shape: another signal proposes, mdb-reset sharpens the start edge
(median 0.38 s), a verifier disposes.

---

## xsdz.51 — Within-show dedup (feed-as-corpus, no second fetch)

**Method.** Same run machinery across DIFFERENT episodes of one show, byte-exact rung,
`min_run_bytes=100000` (≈6.3 s @128 kbps). Cohorts: May A-sides pairwise; July B-sides
pairwise; A-vs-other-episode-B. Each shared run classified on BOTH sides against that
file's independently-known ad regions (gold breaks ∪ xsdz.44 byte-slot map; B-side slot
maps derived from the same-episode chain gaps) with ±2 s pad; intro/outro = first/last 60 s.

**N.** Morbid 4A+4B (28 pairs, 24 cross-episode), SmartLess 4A+3B (21 pairs, 18
cross-episode). Corpus has only 4 SmartLess A-sides (brief said ×5 — 05-31 does not exist
on disk; no subsampling otherwise).

**Results.**

| Show | Cohort | Pairs with hits | Shared (side-obs) | In ad regions | Precision |
|---|---|---|---|---|---|
| morbid | A–A (May) | 2/6 | 217.2 s | 217.2 s | 1.000 |
| morbid | B–B (July, same-day) | 3/6 | 1447.5 s | 1447.5 s | 1.000 |
| smartless | A–A (May) | 2/6 | 151.6 s | 121.6 s | 0.802 |
| smartless | B–B (July, same-day) | 1/3 | 181.9 s | 181.9 s | 1.000 |
| **overall** | | **8/21** | **1998.2 s** | **1968.2 s** | **0.985** |

- The single sub-1.0 cell is ONE 30 s run-side at 4027 s in smartless-05-25 — a file with
  **no B-side slot map** (no fresh fetch exists for it) and PARTIAL gold; its position
  (73 s before EOF, adjacent to the episode's outro ad block) makes an unlabeled outro ad
  the likely reading. Not a proven false positive; not provable content either — reported
  as unknown.
- A-vs-other-B pairs: 0 shared runs everywhere (May creatives no longer in July rotation
  — consistent with campaign turnover).
- **Fetch-time adjacency is the yield lever:** same-day B–B pairs share 212–264 s each
  (whole live campaign pool: prerolls, midroll creatives, show intro/outro stingers —
  360 s of intro-region and 176 s of outro-region material in morbid B–B), while
  cross-week A–A pairs share only 15–80 s.
- Byte-exact (rung 1) sufficed; the frame-payload fallback rung was not needed and was
  not run (per the rung protocol).

**Verdict: GO.** Concatenated cached creatives are byte-identical across episodes, and
what dedup finds is essentially *only* inserted objects (98.5% in ad regions). As a
label source it is rediff-grade in precision but partial in coverage per single pair;
fetching a show's back-catalog in one session maximizes yield with NO ≥24 h re-fetch.

---

## Cross-cutting notes / what changes rediff activation

1. **Replace fpcalc with byte alignment in the truth differ.** Same A/B inputs, no
   decode, ~2 s per episode, monotonic on the Morbid failure class → the 07-17 pilot's
   4 Morbid + (probably) the SmartLess repeated-bed failures come back into the
   measurable N, and slot edges tighten from ±19–30 s (median) to ±0.3 s — inside gold's
   own attestation tolerance. fpcalc can remain as acoustic fallback for shows that
   re-encode per fetch (none seen in this 7-pair set, but doac's mdb profile hints its
   pipeline differs — verify before assuming byte-identity there).
2. **Multi-B union.** The smartless-05-11 15 s undersized slots show single-pair
   byte-rediff inherits the re-served-creative blind spot. Two B-fetches ≥1 week apart
   (or B ∪ same-day sibling-episode dedup evidence) close it.
3. **Composite width oracle:** 44 slots (exact, needs a second fetch) + 51 shared-run
   objects (exact, needs only the feed's other episodes) + 43 mdb-reset (day-0
   start-edge snapping on clean encoders) cover the day-0 → day-1 spectrum from one
   parser with zero audio decoding.
4. Scar-class negatives are corpus-wide facts worth recording in the stinger hub: DAI
   output here contains **no** mid-file tag frames, embedded ID3, resyncs, or codec
   param changes — future "structural anomaly" proposals should be checked against this
   null before anyone builds on them.

## Artifacts

- `scripts/l2f-mp3-forensics.py` — parser + `parse`/`scars`/`align`/`dedup` (uncommitted).
- Scratch `spike-data/`: `scars-gold35.json`, `scars-edges.json`, `align-<ep>.json` ×8
  (incl. techcrunch smoke pair), `dedup-morbid-full.json`, `dedup-smartless-full.json`,
  `dedup-classified.json`, analysis scripts (`scars-deepdive.py`, `score-align.py`,
  `dedup-classify.py`), morbid B-sides (`*.freshdl.mp3`, keep — expiring URLs),
  smartless B-side copies (`*.fresh.mp3`).
- Budget: downloads 297 MB (≤400 MB cap), scratch ~990 MB → ~500 MB after cleanup of
  the 13 unused staged fresh copies (≤1.5 GB cap). No decoded intermediates existed
  (no decoding anywhere in the spike).
