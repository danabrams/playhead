# R3 Auto-Promotion Audit — 2026-06-01

Pattern review of every R3 (rediff-only, ≥20s, `audit_priority=1`) span currently committed to the corpus. Goal: decide whether the R3 promotion threshold in `scripts/l2f-auto-promote.py` should be tightened **before** tomorrow's overnight loop generates more spans.

**N = 71** R3 spans across 25 episodes and 12 shows.

## Per-span table

| # | Show | Span (s) | Dur | Pos | From start | From end | Notes |
|---|------|----------|-----|-----|------------|----------|-------|
| 1 | Fresh Air | 1179–1269 | 90s | 36.5% | 1179.4s | 1964.3s | `fresh-air-2026-05-30-best-of-borou` |
| 2 | Fresh Air | 1994–2071 | 77s | 61.7% | 1994.0s | 1162.3s | `fresh-air-2026-05-30-best-of-borou` |
| 3 | Fresh Air | 3163–3196 | 34s | 97.8% | 3162.6s | 37.3s | `fresh-air-2026-05-30-best-of-borou` |
| 4 | Hard Fork | 0–79 | 79s | 0.0% | 0.0s | 3766.4s | `hard-fork-2026-05-29-interesting-t` |
| 5 | Hard Fork | 1294–1350 | 56s | 33.6% | 1293.7s | 2495.3s | `hard-fork-2026-05-29-interesting-t` |
| 6 | Hard Fork | 1351–1388 | 37s | 35.1% | 1350.7s | 2457.4s | `hard-fork-2026-05-29-interesting-t` |
| 7 | Hard Fork | 2523–2553 | 30s | 65.6% | 2523.5s | 1291.7s | `hard-fork-2026-05-29-interesting-t` |
| 8 | Hard Fork | 2582–2615 | 33s | 67.1% | 2581.6s | 1230.3s | `hard-fork-2026-05-29-interesting-t` |
| 9 | Hard Fork | 3670–3700 | 30s | 95.4% | 3669.8s | 144.9s | `hard-fork-2026-05-29-interesting-t` |
| 10 | Hard Fork | 3728–3761 | 33s | 97.0% | 3727.9s | 84.2s | `hard-fork-2026-05-29-interesting-t` |
| 11 | Morbid | 27–74 | 47s | 0.8% | 26.6s | 3361.2s | `morbid-2026-05-28-listener-tales-1` |
| 12 | Morbid | 1105–1189 | 84s | 31.9% | 1105.1s | 2276.9s | `morbid-2026-05-25-the-matamoros-de` |
| 13 | Morbid | 2442–2500 | 58s | 70.4% | 2441.6s | 966.1s | `morbid-2026-05-25-the-matamoros-de` |
| 14 | Morbid | 2488–2571 | 84s | 66.1% | 2487.8s | 1194.4s | `morbid-2026-05-21-the-matamoros-de` |
| 15 | On The Media | 122–150 | 27s | 3.7% | 122.3s | 3121.6s | `on-the-media-2026-05-29-trump-sued` |
| 16 | On The Media | 1040–1168 | 128s | 31.8% | 1039.7s | 2103.6s | `on-the-media-2026-05-29-trump-sued` |
| 17 | On The Media | 2382–2449 | 67s | 72.8% | 2382.4s | 821.9s | `on-the-media-2026-05-29-trump-sued` |
| 18 | Planet Money | 1456–1488 | 32s | 60.9% | 1455.6s | 904.2s | `planet-money-2026-05-29-the-sneaky` |
| 19 | SmartLess | 0–55 | 55s | 0.0% | 0.0s | 3408.2s | `smartless-2026-05-21-quot-re-relea` |
| 20 | SmartLess | 13–109 | 96s | 0.3% | 12.9s | 3636.7s | `smartless-2026-05-11-quot-kareem-r` |
| 21 | SmartLess | 28–91 | 63s | 0.7% | 28.1s | 3815.8s | `smartless-2026-05-18-quot-sting-qu` |
| 22 | SmartLess | 1180–1352 | 171s | 31.5% | 1180.1s | 2393.8s | `smartless-2026-05-11-quot-kareem-r` |
| 23 | SmartLess | 1398–1447 | 49s | 35.8% | 1397.8s | 2460.4s | `smartless-2026-05-18-quot-sting-qu` |
| 24 | SmartLess | 2566–2629 | 63s | 65.7% | 2565.9s | 1278.0s | `smartless-2026-05-18-quot-sting-qu` |
| 25 | SmartLess | 2589–2661 | 72s | 69.1% | 2589.0s | 1084.7s | `smartless-2026-05-11-quot-kareem-r` |
| 26 | SmartLess | 2703–2737 | 35s | 65.8% | 2702.8s | 1370.3s | `smartless-2026-05-25-quot-nick-jon` |
| 27 | SmartLess | 3970–4035 | 65s | 96.7% | 3970.3s | 72.7s | `smartless-2026-05-25-quot-nick-jon` |
| 28 | The Daily Show: Ears Edition | 718–739 | 20s | 30.0% | 718.3s | 1657.1s | `the-daily-show-ears-edition-2026-0` |
| 29 | The Daily Show: Ears Edition | 880–905 | 25s | 36.7% | 879.8s | 1490.8s | `the-daily-show-ears-edition-2026-0` |
| 30 | The Daily Show: Ears Edition | 1078–1111 | 33s | 45.0% | 1077.9s | 1284.8s | `the-daily-show-ears-edition-2026-0` |
| 31 | The Daily Show: Ears Edition | 1694–1726 | 32s | 70.7% | 1694.4s | 669.4s | `the-daily-show-ears-edition-2026-0` |
| 32 | The Daily Show: Ears Edition | 2237–2268 | 31s | 93.4% | 2236.6s | 128.0s | `the-daily-show-ears-edition-2026-0` |
| 33 | The Ezra Klein Show | 0–30 | 30s | 0.0% | 0.0s | 4634.4s | `the-ezra-klein-show-2026-05-29-doe` |
| 34 | The Mel Robbins Podcast | 199–231 | 32s | 4.0% | 198.8s | 4772.2s | `the-mel-robbins-podcast-2026-06-01` |
| 35 | The Mel Robbins Podcast | 1888–1922 | 34s | 37.7% | 1888.4s | 3081.4s | `the-mel-robbins-podcast-2026-06-01` |
| 36 | The Mel Robbins Podcast | 1924–2004 | 80s | 38.5% | 1924.0s | 2999.8s | `the-mel-robbins-podcast-2026-06-01` |
| 37 | The Mel Robbins Podcast | 2643–2805 | 162s | 52.8% | 2643.0s | 2198.5s | `the-mel-robbins-podcast-2026-06-01` |
| 38 | The Mel Robbins Podcast | 2819–2851 | 33s | 56.3% | 2818.5s | 2152.3s | `the-mel-robbins-podcast-2026-06-01` |
| 39 | The Mel Robbins Podcast | 2854–3211 | 357s | 57.0% | 2854.3s | 1792.4s | `the-mel-robbins-podcast-2026-06-01` |
| 40 | The Mel Robbins Podcast | 4914–4951 | 37s | 98.2% | 4913.5s | 52.7s | `the-mel-robbins-podcast-2026-06-01` |
| 41 | The Mel Robbins Podcast | 4953–4981 | 28s | 99.0% | 4952.7s | 22.6s | `the-mel-robbins-podcast-2026-06-01` |
| 42 | The Nikki Glaser Podcast | 63–125 | 62s | 1.6% | 62.7s | 3860.4s | `the-nikki-glaser-podcast-2025-03-0` |
| 43 | The Nikki Glaser Podcast | 1099–1543 | 445s | 24.8% | 1098.7s | 2890.4s | `the-nikki-glaser-podcast-2025-02-2` |
| 44 | The Nikki Glaser Podcast | 1235–1297 | 62s | 28.7% | 1235.4s | 3007.4s | `the-nikki-glaser-podcast-2025-03-0` |
| 45 | The Nikki Glaser Podcast | 1524–1600 | 76s | 38.2% | 1524.1s | 2385.3s | `the-nikki-glaser-podcast-2025-03-0` |
| 46 | The Nikki Glaser Podcast | 1694–1726 | 31s | 31.7% | 1694.3s | 3627.0s | `the-nikki-glaser-podcast-2025-03-1` |
| 47 | The Nikki Glaser Podcast | 1784–1804 | 20s | 33.3% | 1784.0s | 3548.3s | `the-nikki-glaser-podcast-2025-03-1` |
| 48 | The Nikki Glaser Podcast | 2677–2709 | 31s | 50.0% | 2677.5s | 2643.8s | `the-nikki-glaser-podcast-2025-03-1` |
| 49 | The Nikki Glaser Podcast | 2767–2932 | 165s | 51.7% | 2767.2s | 2420.5s | `the-nikki-glaser-podcast-2025-03-1` |
| 50 | The Nikki Glaser Podcast | 2924–3015 | 91s | 67.9% | 2924.2s | 1289.6s | `the-nikki-glaser-podcast-2025-03-0` |
| 51 | The Nikki Glaser Podcast | 3016–3071 | 55s | 70.1% | 3016.0s | 1233.2s | `the-nikki-glaser-podcast-2025-03-0` |
| 52 | The Nikki Glaser Podcast | 3043–3210 | 166s | 76.4% | 3043.4s | 775.5s | `the-nikki-glaser-podcast-2025-03-0` |
| 53 | The Nikki Glaser Podcast | 3096–3128 | 32s | 68.6% | 3096.0s | 1387.1s | `the-nikki-glaser-podcast-2025-02-2` |
| 54 | The Nikki Glaser Podcast | 3156–3225 | 69s | 69.9% | 3155.7s | 1289.9s | `the-nikki-glaser-podcast-2025-02-2` |
| 55 | The Nikki Glaser Podcast | 3200–3286 | 85s | 72.2% | 3200.4s | 1148.0s | `the-nikki-glaser-podcast-2025-02-2` |
| 56 | Up First | 279–365 | 86s | 9.3% | 278.9s | 2643.7s | `up-first-2026-06-01-can-graham-pla` |
| 57 | Up First | 1993–2032 | 39s | 66.2% | 1992.9s | 976.9s | `up-first-2026-06-01-can-graham-pla` |
| 58 | Why Is This Happening? The Chris Hayes Podcast | 0–48 | 48s | 0.0% | 0.0s | 3792.6s | `why-is-this-happening-the-chris-ha` |
| 59 | Why Is This Happening? The Chris Hayes Podcast | 0–50 | 50s | 0.0% | 0.0s | 3028.8s | `why-is-this-happening-the-chris-ha` |
| 60 | Why Is This Happening? The Chris Hayes Podcast | 0–30 | 30s | 0.0% | 0.0s | 3471.4s | `why-is-this-happening-the-chris-ha` |
| 61 | Why Is This Happening? The Chris Hayes Podcast | 28–58 | 30s | 1.5% | 28.1s | 1878.9s | `why-is-this-happening-the-chris-ha` |
| 62 | Why Is This Happening? The Chris Hayes Podcast | 84–195 | 111s | 2.4% | 83.8s | 3355.7s | `why-is-this-happening-the-chris-ha` |
| 63 | Why Is This Happening? The Chris Hayes Podcast | 92–344 | 252s | 2.6% | 91.7s | 3157.2s | `why-is-this-happening-the-chris-ha` |
| 64 | Why Is This Happening? The Chris Hayes Podcast | 345–599 | 254s | 9.9% | 344.9s | 2901.9s | `why-is-this-happening-the-chris-ha` |
| 65 | Why Is This Happening? The Chris Hayes Podcast | 612–642 | 30s | 17.5% | 611.9s | 2859.1s | `why-is-this-happening-the-chris-ha` |
| 66 | Why Is This Happening? The Chris Hayes Podcast | 747–810 | 63s | 21.3% | 746.9s | 2691.4s | `why-is-this-happening-the-chris-ha` |
| 67 | Why Is This Happening? The Chris Hayes Podcast | 1499–1577 | 78s | 42.8% | 1499.2s | 1923.9s | `why-is-this-happening-the-chris-ha` |
| 68 | Why Is This Happening? The Chris Hayes Podcast | 1905–1933 | 28s | 98.4% | 1905.1s | 3.3s | `why-is-this-happening-the-chris-ha` |
| 69 | Why Is This Happening? The Chris Hayes Podcast | 2512–2591 | 79s | 81.6% | 2512.1s | 488.0s | `why-is-this-happening-the-chris-ha` |
| 70 | Why Is This Happening? The Chris Hayes Podcast | 3110–3220 | 110s | 81.0% | 3110.1s | 621.1s | `why-is-this-happening-the-chris-ha` |
| 71 | Why Is This Happening? The Chris Hayes Podcast | 3771–3792 | 21s | 98.2% | 3771.2s | 49.1s | `why-is-this-happening-the-chris-ha` |

## Distributions

- **Duration (s)** (n=71): min=20.4, p25=32.1, median=55.4, p75=79.7, max=444.6, mean=75.0
- **Distance from start (s)** (n=71): min=0.0, p25=718.3, median=1694.4, p75=2677.5, max=4952.7, mean=1747.1
- **Distance from end (s)** (n=71): min=3.3, p25=1148.0, median=1964.3, p75=2999.8, max=4772.2, mean=1988.6
- **Position-in-episode (fraction 0-1)** (n=71): min=0.0, p25=0.2, median=0.4, p75=0.7, max=1.0, mean=0.5

**Position buckets**:
- First 5% of episode: 15/71
- Middle 90%: 48/71
- Last 5% of episode: 8/71

**Duration buckets**:
- 20–<30s: 11/71
- 20–<40s: 30/71
- ≥90s:    13/71

## Show concentration

| Show | Spans | % of N |
|------|-------|--------|
| The Nikki Glaser Podcast | 14 | 20% |
| Why Is This Happening? The Chris Hayes Podcast | 14 | 20% |
| SmartLess | 9 | 13% |
| The Mel Robbins Podcast | 8 | 11% |
| Hard Fork | 7 | 10% |
| The Daily Show: Ears Edition | 5 | 7% |
| Morbid | 4 | 6% |
| Fresh Air | 3 | 4% |
| On The Media | 3 | 4% |
| Up First | 2 | 3% |
| Planet Money | 1 | 1% |
| The Ezra Klein Show | 1 | 1% |

## Same-episode clustering

- Episodes with >1 R3 span: 19/25 (65 of 71 spans)
  - `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol`: 8 spans
  - `hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi`: 7 spans
  - `why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with`: 6 spans
  - `the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve`: 5 spans
  - `the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit`: 4 spans
  - `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros`: 3 spans
  - `on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8`: 3 spans
  - `smartless-2026-05-11-quot-kareem-rahma-quot`: 3 spans
  - `smartless-2026-05-18-quot-sting-quot`: 3 spans
  - `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os`: 3 spans
  - `the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m`: 3 spans
  - `why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi`: 3 spans
  - `morbid-2026-05-25-the-matamoros-devil-murders-part-2`: 2 spans
  - `smartless-2026-05-25-quot-nick-jonas-quot`: 2 spans
  - `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t`: 2 spans
  - `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev`: 2 spans
  - `up-first-2026-06-01-can-graham-platner-survive-another-contr`: 2 spans
  - `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit`: 2 spans
  - `why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein`: 2 spans

## Findings summary

- **F2. Strong same-episode clustering.** 19 of 25 episodes have >1 R3 span, accounting for 65 of 71 spans (92%). `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` has the most with 8 spans. Could be genuine DAI density (multiple ad breaks per episode), or a per-episode artifact (recurring music bed). Worth ear-witnessing the heaviest episode first.

- **F4. 8/71 R3 spans land in the last 5% of the episode.** Outro stingers / end-cards can sometimes masquerade as DAI ads on the rediff. With a sample this small, this is suggestive, not actionable.

- **F5. 15/71 R3 spans land in the first 5% of the episode.** Pre-roll DAI is real and expected; this is not by itself a red flag.

## Proposed R3 tightening

R3 currently fires when a rediff-only span has length ≥ 20s, with no overlap from drafter or pipeline. Three candidate tightenings are listed below. **Counterfactuals are over the current N = 71 spans only**; extrapolation to future spans is necessarily speculative.

### Option A — raise R3 minimum from 20s → 30s
- Would drop **11 of 71** current R3 spans (15%).
- Future hypothetical: any rediff-only slot in 20–<30s would be rejected entirely (not even queued for audit). MP3 frame alignment artifacts cluster in this band; legitimate ≥30s DAI spots would still promote.
- **Reversibility**: easy — single integer constant in `l2f-auto-promote.py`. No corpus migration required for future runs (existing spans are already committed).

### Option B — raise R3 minimum from 20s → 40s
- Would drop **30 of 71** current R3 spans (42%).
- More aggressive. Loses recall on 30–<40s DAI inserts that are real but short (some pre-rolls run 30s).

### Option C — add a tail guard (drop R3 if position > 0.98)
- Would drop **4 of 71** current R3 spans.
- Motivated by end-card / outro music tripping rediff. With the current sample, this is at most weakly indicated.

### Option D — reject R3 spans whose end_seconds > episode duration_seconds
- Would drop **0 of 71** current R3 spans (0%).
- Targets the F_PAST pattern: rediff overshoot past last audible frame. The end boundary is definitely wrong for these spans; whether the start is a real ad break is unknown. Rejecting the span entirely is conservative but may discard real ad-break starts.
- **Reversibility**: easy — add a guard condition before the R3 promotion path in `l2f-auto-promote.py`.

### Recommendation

**No threshold change is justified by the evidence alone.** With N = 71 and zero ground-truth-listening confirmations, the show/episode concentration (F1, F2) could be signal or could be artifact, and the duration bucket counts are too thin to commit to a permanent change.

Recommended next step is to **ear-witness a sample of the 71 current audit_priority=1 spans** — prioritizing the heaviest-hit episodes — with `scripts/l2f-audit-queue.py` and ffplay. Classify each as ad vs. host, and only THEN decide whether to tighten R3. The reject log (`scripts/l2f-flag-false-promote.py`) already captures veto decisions, so the cost of one audit pass is small and the information gain is high.

If a human-audit pass is not possible before the overnight loop runs and false-promotion drift is the larger concern, **Option A (≥30s)** is the most conservative tightening: it would drop only the 11 shortest current spans while leaving every ≥30s span untouched. Option B (≥40s) drops more and is harder to justify without audit data. **Option D (reject past-end)** is also low-risk: the end boundary is definitively wrong for those spans, and rejecting them prevents a known class of malformed windows from accumulating in the corpus — but it does not address the underlying rediff overshoot.

## Caveats

- **N = 71 is tiny.** Any apparent pattern (show share, duration distribution, edge concentration) is fully consistent with random variance at this sample size. Treat the findings as hypotheses, not conclusions.
- **No ground truth.** These spans have not been ear-witnessed. We do not know which are real DAI ads, which are host content, and which are MP3 frame artifacts. The pattern analysis cannot distinguish a real DAI-heavy show from a show that breaks the rediff signal.
- **Selection bias from R1/R2.** R3 only fires when the drafter AND pipeline both missed the span. Shows with weaker transcripts or noisier pipelines will be over-represented in R3 by construction, independent of actual ad density.
- **Threshold-tightening is asymmetric.** Raising R3 from 20s → 30s drops true positives along with false positives; without ground-truth labels we cannot estimate the precision/recall trade-off.

---

## Corpus Quality Scan (All Provenances)

Cross-provenance anti-pattern scan added 2026-06-01 after PR #212 discovered a F_PAST artifact (TED Business R1 span, overshoot 44s) that the original R3-only audit missed because it filtered to `audit_priority=1`. This section scans EVERY `ad_window` regardless of which rule promoted it.

**No artifacts detected.** Corpus is clean across all four categories.
