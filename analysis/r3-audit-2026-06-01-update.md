# R3 Auto-Promotion Audit — 2026-06-01 (N=27 Update)

> **Historical record**: The original N=8 audit is preserved at
> `analysis/r3-audit-2026-06-01.md`. That report (from PR #204)
> concluded "no threshold change is justified" with N=8 across 4
> episodes and 4 shows. This update re-runs the same audit after
> the corpus tripled to 27 R3 spans via a manual loop fire that
> produced 9 new annotations (TechCrunch ×3, Stuff You Should Know,
> Daily Show, Nikki Glaser back-catalog ×4). Current annotation
> state: 16 annotation files, 66 ad-windows (39 triangulated + 27
> audit-priority).

## What changed since N=8

| Metric | N=8 (original) | N=27 (this update) |
|--------|----------------|--------------------|
| Shows | 4 | 5 |
| Episodes | 4 | 9 |
| Top show (Nikki Glaser) | 4/8 = 50% | 18/27 = 67% |
| Clustering (spans in multi-span eps) | 6/8 = 75% | 25/27 = 93% |
| Spans past episode end | 1/8 = 12% | 5/27 = 19% |
| Short spans 20–<30s | 1/8 = 12% | 3/27 = 11% |
| Short spans 20–<40s | 4/8 = 50% | 10/27 = 37% |
| New show: The Daily Show: Ears Edition | — | 5 spans |

Key shift: Nikki Glaser concentration increased from 50% to 67% as
4 more back-catalog episodes were added. The Daily Show is a new
entrant with 5 spans from a single episode — the second most
concentrated. The F_PAST (past-episode-end) finding is new in this
report; the original sanity flags section only captured 1 case.

---

Pattern review of every R3 (rediff-only, ≥20s, `audit_priority=1`) span currently committed to the corpus. Goal: decide whether the R3 promotion threshold in `scripts/l2f-auto-promote.py` should be tightened **before** tomorrow's overnight loop generates more spans.

**N = 27** R3 spans across 9 episodes and 5 shows.

## Per-span table

| # | Show | Span (s) | Dur | Pos | From start | From end | Notes |
|---|------|----------|-----|-----|------------|----------|-------|
| 1 | Casefile True Crime | 6030–6126 | 96s | 99.5% | 6029.7s | -64.1s | `casefile-true-crime-2026-05-30-cas` |
| 2 | SmartLess | 2703–2737 | 35s | 65.8% | 2702.8s | 1370.3s | `smartless-2026-05-25-quot-nick-jon` |
| 3 | SmartLess | 3970–4035 | 65s | 96.7% | 3970.3s | 72.7s | `smartless-2026-05-25-quot-nick-jon` |
| 4 | The Daily Show: Ears Edition | 718–739 | 20s | 30.0% | 718.3s | 1657.1s | `the-daily-show-ears-edition-2026-0` |
| 5 | The Daily Show: Ears Edition | 880–905 | 25s | 36.7% | 879.8s | 1490.8s | `the-daily-show-ears-edition-2026-0` |
| 6 | The Daily Show: Ears Edition | 1078–1111 | 33s | 45.0% | 1077.9s | 1284.8s | `the-daily-show-ears-edition-2026-0` |
| 7 | The Daily Show: Ears Edition | 1694–1726 | 32s | 70.7% | 1694.4s | 669.4s | `the-daily-show-ears-edition-2026-0` |
| 8 | The Daily Show: Ears Edition | 2237–2268 | 31s | 93.4% | 2236.6s | 128.0s | `the-daily-show-ears-edition-2026-0` |
| 9 | The Nikki Glaser Podcast | 63–125 | 62s | 1.6% | 62.7s | 3860.4s | `the-nikki-glaser-podcast-2025-03-0` |
| 10 | The Nikki Glaser Podcast | 1099–1543 | 445s | 24.8% | 1098.7s | 2890.4s | `the-nikki-glaser-podcast-2025-02-2` |
| 11 | The Nikki Glaser Podcast | 1235–1297 | 62s | 28.7% | 1235.4s | 3007.4s | `the-nikki-glaser-podcast-2025-03-0` |
| 12 | The Nikki Glaser Podcast | 1524–1600 | 76s | 38.2% | 1524.1s | 2385.3s | `the-nikki-glaser-podcast-2025-03-0` |
| 13 | The Nikki Glaser Podcast | 1694–1726 | 31s | 31.7% | 1694.3s | 3627.0s | `the-nikki-glaser-podcast-2025-03-1` |
| 14 | The Nikki Glaser Podcast | 1784–1804 | 20s | 33.3% | 1784.0s | 3548.3s | `the-nikki-glaser-podcast-2025-03-1` |
| 15 | The Nikki Glaser Podcast | 2677–2709 | 31s | 50.0% | 2677.5s | 2643.8s | `the-nikki-glaser-podcast-2025-03-1` |
| 16 | The Nikki Glaser Podcast | 2767–2932 | 165s | 51.7% | 2767.2s | 2420.5s | `the-nikki-glaser-podcast-2025-03-1` |
| 17 | The Nikki Glaser Podcast | 2924–3015 | 91s | 67.9% | 2924.2s | 1289.6s | `the-nikki-glaser-podcast-2025-03-0` |
| 18 | The Nikki Glaser Podcast | 3016–3071 | 55s | 70.1% | 3016.0s | 1233.2s | `the-nikki-glaser-podcast-2025-03-0` |
| 19 | The Nikki Glaser Podcast | 3043–3210 | 166s | 76.4% | 3043.4s | 775.5s | `the-nikki-glaser-podcast-2025-03-0` |
| 20 | The Nikki Glaser Podcast | 3096–3128 | 32s | 68.6% | 3096.0s | 1387.1s | `the-nikki-glaser-podcast-2025-02-2` |
| 21 | The Nikki Glaser Podcast | 3156–3225 | 69s | 69.9% | 3155.7s | 1289.9s | `the-nikki-glaser-podcast-2025-02-2` |
| 22 | The Nikki Glaser Podcast | 3200–3286 | 85s | 72.2% | 3200.4s | 1148.0s | `the-nikki-glaser-podcast-2025-02-2` |
| 23 | The Nikki Glaser Podcast | 4143–4190 | 47s | 104.0% | 4143.0s | -204.4s | `the-nikki-glaser-podcast-2025-03-0` |
| 24 | The Nikki Glaser Podcast | 4333–4413 | 80s | 100.7% | 4333.1s | -108.9s | `the-nikki-glaser-podcast-2025-03-0` |
| 25 | The Nikki Glaser Podcast | 4517–4579 | 62s | 101.9% | 4517.1s | -145.8s | `the-nikki-glaser-podcast-2025-02-2` |
| 26 | The Nikki Glaser Podcast | 4583–4676 | 93s | 101.5% | 4582.8s | -161.2s | `the-nikki-glaser-podcast-2025-02-2` |
| 27 | Why Is This Happening? The Chris Hayes Podcast | 84–195 | 111s | 2.4% | 83.8s | 3355.7s | `why-is-this-happening-the-chris-ha` |

## Distributions

- **Duration (s)** (n=27): min=20.4, p25=31.8, median=62.1, p75=90.7, max=444.6, mean=78.6
- **Distance from start (s)** (n=27): min=62.7, p25=1235.4, median=2702.8, p75=3200.4, max=6029.7, mean=2527.7
- **Distance from end (s)** (n=27): min=-204.4, p25=128.0, median=1289.9, p75=2643.8, max=3860.4, mean=1513.0
- **Position-in-episode (fraction 0-1)** (n=27): min=0.0, p25=0.3, median=0.7, p75=0.9, max=1.0, mean=0.6

**Position buckets**:
- First 5% of episode: 2/27
- Middle 90%: 19/27
- Last 5% of episode: 6/27

**Duration buckets**:
- 20–<30s: 3/27
- 20–<40s: 10/27
- ≥90s:    7/27

## Show concentration

| Show | Spans | % of N |
|------|-------|--------|
| The Nikki Glaser Podcast | 18 | 67% |
| The Daily Show: Ears Edition | 5 | 19% |
| SmartLess | 2 | 7% |
| Casefile True Crime | 1 | 4% |
| Why Is This Happening? The Chris Hayes Podcast | 1 | 4% |

## Same-episode clustering

- Episodes with >1 R3 span: 7/9 (25 of 27 spans)
  - `the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve`: 5 spans
  - `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os`: 4 spans
  - `the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m`: 4 spans
  - `the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit`: 4 spans
  - `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t`: 3 spans
  - `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev`: 3 spans
  - `smartless-2026-05-25-quot-nick-jonas-quot`: 2 spans

## Sanity flags

Spans whose `end_seconds` exceeds the annotated `duration_seconds` (likely rediff cluster overshoot past last audible frame — not necessarily wrong, but worth a manual look):
- `casefile-true-crime-2026-05-30-case-340-elisabeth-membrey`: span ends 64.1s past episode end (6125.7s vs 6061.6s)
- `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t`: span ends 145.8s past episode end (4579.5s vs 4433.7s)
- `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev`: span ends 161.2s past episode end (4676.1s vs 4514.9s)
- `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os`: span ends 108.9s past episode end (4413.4s vs 4304.5s)
- `the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m`: span ends 204.4s past episode end (4189.6s vs 3985.2s)

## Findings summary

- **F1. Show concentration is real.** 18 of 27 R3 spans (67%) come from a single show (*The Nikki Glaser Podcast*). Either (a) that show is genuinely heavy on DAI-only ad slots that drafter+pipeline both miss, or (b) there's a show-specific artifact (loud music stings, intro/outro stingers) tripping the rediff signal. Both are plausible without listening; cannot disambiguate from the annotations alone.

- **F2. Strong same-episode clustering.** 7 of 9 episodes have >1 R3 span, accounting for 25 of 27 spans (93%). `the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve` has the most with 5 spans. Could be genuine DAI density (multiple ad breaks per episode), or a per-episode artifact (recurring music bed). Worth ear-witnessing the heaviest episode first.

- **F_PAST. 5 of 27 R3 spans (19%) extend past the annotated `duration_seconds`.** These are likely rediff cluster overshoot — the rediff algorithm finds a content-difference region that bleeds past the last audible frame, possibly into the MP3 silence/padding at the tail. All 5 past-end spans come from Nikki Glaser back-catalog and Casefile. The overshoot amounts range from ~64s to ~204s. Without ground truth, we cannot say whether the *start* of each span is a genuine ad break; the overshoot only disqualifies the end boundary.

- **F4. 6/27 R3 spans land in the last 5% of the episode.** Outro stingers / end-cards can sometimes masquerade as DAI ads on the rediff. With a sample this small, this is suggestive, not actionable.

- **F5. 2/27 R3 spans land in the first 5% of the episode.** Pre-roll DAI is real and expected; this is not by itself a red flag.

## Proposed R3 tightening

R3 currently fires when a rediff-only span has length ≥ 20s, with no overlap from drafter or pipeline. Three candidate tightenings are listed below. **Counterfactuals are over the current N = 27 spans only**; extrapolation to future spans is necessarily speculative.

### Option A — raise R3 minimum from 20s → 30s
- Would drop **3 of 27** current R3 spans (11%).
- Future hypothetical: any rediff-only slot in 20–<30s would be rejected entirely (not even queued for audit). MP3 frame alignment artifacts cluster in this band; legitimate ≥30s DAI spots would still promote.
- **Reversibility**: easy — single integer constant in `l2f-auto-promote.py`. No corpus migration required for future runs (existing spans are already committed).

### Option B — raise R3 minimum from 20s → 40s
- Would drop **10 of 27** current R3 spans (37%).
- More aggressive. Loses recall on 30–<40s DAI inserts that are real but short (some pre-rolls run 30s).

### Option C — add a tail guard (drop R3 if position > 0.98)
- Would drop **5 of 27** current R3 spans.
- Motivated by end-card / outro music tripping rediff. With the current sample, this is at most weakly indicated.

### Option D — reject R3 spans whose end_seconds > episode duration_seconds
- Would drop **5 of 27** current R3 spans (19%).
- Targets the F_PAST pattern: rediff overshoot past last audible frame. The end boundary is definitely wrong for these spans; whether the start is a real ad break is unknown. Rejecting the span entirely is conservative but may discard real ad-break starts.
- **Reversibility**: easy — add a guard condition before the R3 promotion path in `l2f-auto-promote.py`.

### Recommendation

**Patterns are clearer than at N=8, but still insufficient for a threshold change without ground truth.** The show concentration (18/27 = 67% from *The Nikki Glaser Podcast*), near-total clustering (25/27 spans in multi-span episodes), and 5 past-end overshoot cases (19%) are consistent with either a genuine DAI-heavy show OR a show-level rediff artifact. We cannot distinguish them without listening.

Recommended next step is to **ear-witness a sample of the 27 current audit_priority=1 spans** — prioritizing the heaviest-hit episodes — with `scripts/l2f-audit-queue.py` and ffplay. Classify each as ad vs. host, and only THEN decide whether to tighten R3. The reject log (`scripts/l2f-flag-false-promote.py`) already captures veto decisions, so the cost of one audit pass is small and the information gain is high.

If a human-audit pass is not possible before the overnight loop runs and false-promotion drift is the larger concern, **Option A (≥30s)** is the most conservative tightening: it would drop only the 3 shortest current spans while leaving every ≥30s span untouched. Option B (≥40s) drops more and is harder to justify without audit data. **Option D (reject past-end)** is also low-risk: the end boundary is definitively wrong for those spans, and rejecting them prevents a known class of malformed windows from accumulating in the corpus — but it does not address the underlying rediff overshoot.

## Caveats

- **N = 27 is tiny.** Any apparent pattern (show share, duration distribution, edge concentration) is fully consistent with random variance at this sample size. Treat the findings as hypotheses, not conclusions.
- **No ground truth.** These spans have not been ear-witnessed. We do not know which are real DAI ads, which are host content, and which are MP3 frame artifacts. The pattern analysis cannot distinguish a real DAI-heavy show from a show that breaks the rediff signal.
- **Selection bias from R1/R2.** R3 only fires when the drafter AND pipeline both missed the span. Shows with weaker transcripts or noisier pipelines will be over-represented in R3 by construction, independent of actual ad density.
- **Threshold-tightening is asymmetric.** Raising R3 from 20s → 30s drops true positives along with false positives; without ground-truth labels we cannot estimate the precision/recall trade-off.
